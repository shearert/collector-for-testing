import os
import time
import inspect
import json
import warnings
import sys
import configparser
from time import strftime
from time import localtime
import traceback

import pebble

import pscheduler
import functools
import logging
import multiprocessing
from multiprocessing.dummy import Pool as ThreadPool

from .esmond.api.client.perfsonar.query import ApiFilters
from .esmond.api.client.perfsonar.query import ApiConnect, ApiConnectWarning

from perfsonar_collector.mesh import Mesh
from perfsonar_collector.monitoring import timed_execution, Monitoring
from perfsonar_collector.translate import Uploader

MINUTES = 60


class Scheduler(object):
    
    def __init__(self,cp,pool,log):
        self.pool = pool
        self.cp = cp
        self.futures = {}
        self.log = log
        self.meshes = {}
        self.oneshot = False
        
        
        
    def query_ps_child(cp, endpoint, oneshot = False, query_range = ()):
        reverse_dns = endpoint.split(".")
        reverse_dns = ".".join(reverse_dns[::-1])
        log = logging.getLogger("perfSonar.{}".format(reverse_dns))
        log.info("I query endpoint {}.".format(endpoint))
        with timed_execution(endpoint):
            if not oneshot:
                uploader = Uploader(connect=endpoint, config=cp, log = log)
            else:
                log.info("In child ps query, backprocessing...{} to {}".format(query_range[0], query_range[1]))

                uploader = Uploader(connect=endpoint, config=cp, log = log, backprocess_start=query_range[0], backprocess_end=query_range[1])
            return uploader.getData()


    def query_ps(state, endpoint):
        old_future = state.futures.get(endpoint)
        if old_future:
            if not old_future.done():
                state.log.info("Prior probe {} is still running; skipping query.".format(endpoint))
                return
            # For now, ignore the result.
            try:
                old_future.result()
            except Exception as e:
                state.log.exception("Failed to get data last time for endpoint {0}: ".format(endpoint))
                Monitoring.SendEndpointFailure(endpoint)
            finally:
                Monitoring.DecRequestsPending()

        timeout = state.cp.getint("Scheduler", "query_timeout") * MINUTE
        result = state.pool.schedule(query_ps_child, args=(state.cp, endpoint, state.oneshot, state.query_range), timeout=timeout)
        Monitoring.IncRequestsPending()
        state.futures[endpoint] = result

        # Check for the oneshot reprocess
        if isOneShot(state.cp):
            return schedule.CancelJob
        
        
###Takes mesh and grabs end points to be passed onto query_ps for actual query        
    def query_ps_mesh(state):
        state.log.info("Querying PS mesh")

        endpoints = set()
        for mesh in state.meshes:
            try:
                cur_mesh = Mesh(mesh)
                state.meshes[mesh] = cur_mesh.get_nodes()
            except Exception as exc:
                # Very generic, but catch all the possible connection issues with the mesh
                # Set the endpoints to what it was before, no changes
                state.log.exception("Failed to get nodes from the mesh: %s, using the previously known nodes", mesh)
                if mesh not in state.meshes:
                    state.meshes[mesh] = []
            endpoints |= set(state.meshes[mesh])


        state.log.info("Nodes: %s", endpoints)

        running_probes = set(state.probes)
        probes_to_stop = running_probes.difference(endpoints)
        probes_to_start = endpoints.difference(running_probes)

        for probe in probes_to_stop:
            state.log.debug("Stopping probe: %s", probe)
            Monitoring.DecNumEndpoints()
            state.probes.remove(probe)
            schedule.clear(probe)
            future = state.futures.get(probe)
            if not future:
                continue
            if future.done():
                future.result()
            else:
                future.cancel()

        default_probe_interval = state.cp.getint("Scheduler", "probe_interval") * MINUTE

        for probe in probes_to_start:
            state.log.debug("Adding probe: %s", probe)
            Monitoring.IncNumEndpoints()
            state.probes.add(probe)
            probe_interval = default_probe_interval
            if state.cp.has_section(probe) and state.cp.has_option("interval"):
                probe_interval = state.cp.getint(probe, "interval") * MINUTE

            query_ps_job = functools.partial(query_ps, state, probe)
            # Run the probe the first time
            query_ps_job()
            if not isOneShot(state.cp):
                schedule.every(probe_interval).to(probe_interval + MINUTE).seconds.do(query_ps_job).tag(probe)

        time.sleep(5)
        logging.debug("Finished querying mesh")
        
    
    
    def main():
        global MINUTE
        cp = perfsonar_collector.config.get_config()
        if cp.has_option("Scheduler","debug"):
            if cp.get("Scheduler", "debug").lower() == "true":
                MINUTE = 1
        perfsonar_collector.config.setup_logging(cp)
        global log
        log = logging.getLogger("scheduler")
        
        
        pool_size = 5
        if cp.has_option("Scheduler", "pool_size"):
            pool_size = cp.getint("Scheduler", "pool_size")
        pool = pebble.ProcessPool(max_workers=pool_size,max_tasks=5)
        
        state = SchedulerState(cp,pool,log)
        
        mesh_config_val = state.cp.get("Mesh", "endpoint")
        if "," in mesh_config_val:
            meshes = mesh_config_val.split(",")
        else:
            meshes = [mesh_config_val]

        for mesh in meshes:
            state.meshes[mesh] = []

        # Query the mesh the first time
        query_ps_mesh(state)
        
        mesh_interval_s = cp.getint("Scheduler", "mesh_interval") * MINUTE
        cleanup_futures_job = functools.partial(cleanup_futures, state)

        query_ps_mesh_job = functools.partial(query_ps_mesh, state)
        schedule.every(mesh_interval_s).to(mesh_interval_s + MINUTE).seconds.do(query_ps_mesh_job)
        schedule.every(10).seconds.do(cleanup_futures_job)
        
        monitor = Monitoring()
        
        try:
            while True:
                schedule.run_pending()
                monitor.process_messages()
                time.sleep(1)
            else:
                pool.close()
                pool.join()
        except:
            pool.stop()
            pool.join()
            raise