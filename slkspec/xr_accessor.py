from itertools import compress

import dask
import xarray as xr


@xr.register_dataarray_accessor("stage")
@xr.register_dataset_accessor("stage")
class stage:
    def __init__(self, xarray_obj):

        self._obj = xarray_obj

    def __call__(self):

        if isinstance(self._obj, xr.Dataset):
            return self.get_dataset()
        elif isinstance(self._obj, xr.DataArray):
            return self.get_dataarray()

    def check_layer(self, graph, layer_identifier="open_dataset"):
        return bool(graph.startswith(layer_identifier))

    def get_output_layers(self, highLevelGraph):
        k = list(highLevelGraph.keys())
        layer_mask = list(map(self.check_layer, highLevelGraph.keys()))

        return list(compress(k, layer_mask))

    def get_output_keys(self, graph):
        layer_keys = self.get_output_layers(graph.layers)
        layers = [graph.layers[k] for k in layer_keys]
        output_keys = [lay.get_output_keys() for lay in layers]

        return set().union(*output_keys)

    def get_input_graph(self, graph, output_keys):
        input_graph = graph.cull(keys=output_keys)
        return input_graph

    def get_data(self, data):
        def do_nothing(x):
            return

        dask_keys = data.__dask_keys__()
        graph = data.dask.cull(keys=dask_keys)

        output_keys = self.get_output_keys(graph)
        input_graph = self.get_input_graph(graph, output_keys)
        input_graph = input_graph.to_dict()
        for k, key in enumerate(output_keys):
            input_graph[f"do_nothing_w_dataset-{k}"] = (do_nothing, key)
        input_graph["do_nothing_at_all"] = (
            do_nothing,
            [f"do_nothing_w_dataset-{t}" for t in range(k)],
        )
        scheduler = (
            dask.base.get_scheduler()
        )  # determine whether LocalCluster/SLUMCluster etc. exist
        _ = scheduler(input_graph, "do_nothing_at_all")
        return

    def get_dataarray(self):
        return self.get_data(self._obj.data)

    def get_dataset(self):
        for var in self._obj.data_vars:
            _ = self.get_data(self._obj[var].data)
        return
