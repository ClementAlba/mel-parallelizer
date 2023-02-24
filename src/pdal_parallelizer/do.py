"""
Do file.

Responsible for all the executions, serializations or creations of object we need to process the pipelines.
"""
import sys

import dask
import dask.array
from dask.distributed import Lock
import tile
import cloud
import bounds
import pickle
import numpy as np
import os


@dask.delayed
def execute_stages_streaming(stages, arr):
    crop = stages.pop(0).pipeline(arr)
    iterator = crop.iterator()
    arrays = []

    for array in iterator:
        for stage in stages:
            pipeline = stage.pipeline(array)
            pipeline.execute()
            array = pipeline.arrays[0]
        arrays.append(array)

    return arrays


@dask.delayed
def execute_stages_standard(stages):
    readers_stage = stages.pop(0)
    readers = readers_stage.pipeline()
    readers.execute()
    arr = readers.arrays[0]

    for stage in stages:
        pipeline = stage.pipeline(arr)
        pipeline.execute()
        if len(pipeline.arrays[0]) > 0:
            arr = pipeline.arrays[0]

    return arr


@dask.delayed
def write_cloud(array, stage, pipeline=None, temp_dir=None):
    if type(array) is list:
        array = np.concatenate(array)

    stage = stage.pipeline(array)
    stage.execute_streaming()
    if temp_dir:
        try:
            os.remove(temp_dir + '/' + str(pipeline) + '.pickle')
        except FileNotFoundError:
            pass


def process_serialized_pipelines(temp_dir, iterator):
    """Create the array of delayed tasks for pipelines in the temp directory (if there is some)"""
    results = []
    for item in iterator:
        stages = item[1]
        writers = stages.pop()
        arrays = execute_stages_standard(stages)
        result = write_cloud(arrays, writers, item[2], temp_dir)
        results.append(result)

    return results


def process_multiple_clouds(output_dir, json_pipeline, iterator, temp_dir=None, dry_run=None, is_single=False):
    results = []
    for item in iterator:
        t = item if is_single else tile.Tile(item, output_dir, json_pipeline)
        p = t.pipeline(is_single)

        stages = p[1]

        if not dry_run:
            serializePipeline(p, temp_dir)
            writers = stages.pop()
            arrays = execute_stages_standard(stages)
            result = write_cloud(arrays, writers, p[2], temp_dir)
            results.append(result)
        else:
            writers = stages.pop()
            arrays = execute_stages_standard(stages)
            result = write_cloud(arrays, writers)
            results.append(result)

    return results


def process_single_cloud(iterator, image_array):
    results = []
    for item in iterator:
        t = item
        p = t.pipeline(True)

        stages = p[1]
        # Remove the readers bc we read it before
        stages.pop(0)
        writers = stages.pop()

        arrays = execute_stages_streaming(stages, image_array)
        result = write_cloud(arrays, writers)
        results.append(result)

    return results


def splitCloud(filepath, output_dir, json_pipeline, tile_bounds, nTiles=None, buffer=None, remove_buffer=False, bounding_box=None):
    """Split the cloud in many tiles"""
    if bounding_box:
        bds = bounds.Bounds(bounding_box[0], bounding_box[1], bounding_box[2], bounding_box[3])
        # Create a cloud object
        c = cloud.Cloud(filepath, bounds=bds)
    else:
        c = cloud.Cloud(filepath)

    # Create a tile the size of the cloud
    t = tile.Tile(filepath=c.filepath, output_dir=output_dir, json_pipeline=json_pipeline, bounds=c.bounds,
                  buffer=buffer, remove_buffer=remove_buffer, cloud_object=c)
    # Split the tile in small parts of given sizes
    return t.split(tile_bounds[0], tile_bounds[1], nTiles)


def serializePipeline(pipeline, temp_dir):
    """Serialize the pipelines"""
    # Create the temp file path
    temp_file = temp_dir + '/' + str(pipeline[2]) + '.pickle'
    with open(temp_file, 'wb') as outfile:
        # Serialize the pipeline
        pickle.dump(pipeline, outfile)
