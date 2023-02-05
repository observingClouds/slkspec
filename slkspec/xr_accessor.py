from itertools import compress

import dask
import xarray as xr


@xr.register_dataarray_accessor("slk")
@xr.register_dataset_accessor("slk")
class slk:
    """Request files without keeping them in memory.

    This accessor is similar to :py:func:`xarray.Dataset.load`,
    :py:func:`xarray.Dataset.persist` and
    :py:meth:`xarray.Dataset.compute`, but instead of loading the
    dataset into memory or start the computation, it just requests the
    data and immediatly discards it. This allows the backend to trigger
    the retrieval of the data for later usage and can be cached locally
    when using e.g.`simplecache` or `slkspec` with SLK_CACHE.
    """

    def __init__(self, xarray_obj):
        self._obj = xarray_obj

    def stage(self):
        if isinstance(self._obj, xr.Dataset):
            return self._get_dataset()
        elif isinstance(self._obj, xr.DataArray):
            return self._get_dataarray()

    def _check_layer(self, key, layer="open_dataset"):
        """Check if dask graph key is part of `layer`

        Inputs
        ------
        key : str
            Dask task key
        layer : str
            Task layer identifier

        Returns
        -------
        result : bool
            Flag if key belongs to `layer`
        """
        return bool(key.startswith(layer))

    def _get_output_tasks(self, highLevelGraph):
        """Get all tasks that are responsible for loading chunks, i.e. those in
        the `open_dataset` layers in the dask `highLevelGraph`.

        Inputs
        ------
        highLevelGraph : dask.highLevelGraph
            Entire dask task graph

        Returns
        -------
        result : list
            List of open_dataset tasks
        """
        k = list(highLevelGraph.keys())
        layer_mask = list(map(self._check_layer, highLevelGraph.keys()))

        return list(compress(k, layer_mask))

    def _get_output_keys(self, graph):
        layer_keys = self._get_output_tasks(graph.layers)
        layers = [graph.layers[k] for k in layer_keys]
        output_keys = [lay._get_output_keys() for lay in layers]

        return set().union(*output_keys)

    def _get_input_graph(self, graph, output_keys):
        """Simplify dask graph based on `output_keys`"""
        input_graph = graph.cull(keys=output_keys)
        return input_graph

    def _connect_tasks(self, graph, tasks_to_connect):
        """Connect tasks with pseudo gathering the results of sub-tasks.

        Inputs
        ------
        graph : dict
            Dictionary representation of dask task graph
        tasks_to_connect : list
            List of tasks that should be connected embarrassingly parallel.

        Returns
        -------
        graph : dict
            Dask task graph with additional connections
        """

        def do_nothing(x):
            return

        # Adding embarrassingly parallel layer to each task to immediatly
        # free memory
        for k, key in enumerate(tasks_to_connect):
            graph[f"do_nothing_w_dataset-{k}"] = (do_nothing, key)
        # Gather tasks
        graph["do_nothing_at_all"] = (
            do_nothing,
            [f"do_nothing_w_dataset-{t}" for t in range(k)],
        )
        return graph

    def _get_data(self, data):
        """Main function."""
        dask_keys = data.__dask_keys__()
        graph = data.dask.cull(keys=dask_keys)

        output_keys = self._get_output_keys(graph)
        input_graph = self._get_input_graph(graph, output_keys)
        input_graph = input_graph.to_dict()
        input_graph = self._connect_tasks(input_graph, output_keys)
        scheduler = (
            dask.base.get_scheduler()
        )  # determine whether LocalCluster/SLUMCluster etc. exist
        _ = scheduler(input_graph, "do_nothing_at_all")
        return

    def _get_dataarray(self):
        """Get_data wrapper for xr.DataArray."""
        return self._get_data(self._obj.data)

    def _get_dataset(self):
        """Get_data wrapper for xr.Dataset."""
        for var in self._obj.data_vars:
            _ = self._get_data(self._obj[var].data)
        return
