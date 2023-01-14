from itertools import compress

import dask
import xarray as xr


@xr.register_dataset_accessor("stage")
class stage:
    def __init__(self, xarray_obj):

        self._obj = xarray_obj

    def __call__(self):

        return self.get_dataset()

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

    def get_dataset(self, keep_in_memory=False):
        das = {}
        for var in self._obj.data_vars:
            dask_keys = self._obj[var].data.__dask_keys__()
            graph = self._obj[var].data.dask.cull(keys=dask_keys)

            output_keys = self.get_output_keys(graph)
            input_graph = self.get_input_graph(graph, output_keys)
            if keep_in_memory:
                das[var] = dask.threaded.get(input_graph, list(output_keys))
            else:
                _ = dask.threaded.get(input_graph, list(output_keys))
        if keep_in_memory:
            return das
        else:
            return
