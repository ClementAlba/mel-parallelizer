import unittest
from src.pdal_parallelizer import tile, cloud, do, file_manager, bounds
import pdal
import json
import os
import dask
from dask.distributed import Client


class TestBounds(unittest.TestCase):
    def test_getDistX(self):
        data = bounds.Bounds(10, 10, 30, 30)
        result = data.getDistX()
        self.assertEqual(result, 20)

    def test_getDistY(self):
        data = bounds.Bounds(10, 10, 30, 50)
        result = data.getDistY()
        self.assertEqual(result, 40)

    def test_positive_buffer(self):
        data = bounds.Bounds(10, 10, 30, 50)
        result = data.buffer(10)
        self.assertIsInstance(result, tuple)
        coord = result[1]
        self.assertEqual(coord.minx, 0)
        self.assertEqual(coord.miny, 0)
        self.assertEqual(coord.maxx, 40)
        self.assertEqual(coord.maxy, 60)

    def test_negative_buffer(self):
        data = bounds.Bounds(10, 10, 30, 50)
        result = data.buffer(-10)
        self.assertIsInstance(result, tuple)
        coord = result[1]
        self.assertEqual(coord.minx, 0)
        self.assertEqual(coord.miny, 0)
        self.assertEqual(coord.maxx, 40)
        self.assertEqual(coord.maxy, 60)


class TestCloud(unittest.TestCase):
    def test_getCount(self):
        data = cloud.Cloud("test/data/input/echantillon_10pts.laz")
        result = data.getCount()
        self.assertEqual(result, 10)

    def test_bounds(self):
        b = bounds.Bounds(10, 10, 20, 40)
        data = cloud.Cloud("test/data/input/echantillon_10pts.laz", b)
        result = data.bounds
        self.assertEqual(result.minx, b.minx)
        self.assertEqual(result.miny, b.miny)
        self.assertEqual(result.maxx, b.maxx)
        self.assertEqual(result.maxy, b.maxy)

    def test_hasClassFlags(self):
        data = cloud.Cloud("test/data/input/echantillon_10pts.laz")
        result = data.hasClassFlags()
        self.assertTrue(result)


class TestDo(unittest.TestCase):
    with open("test/data/pipeline.json", "r") as p:
        pipeline = json.load(p)
    pipeline = [pdal.Pipeline(json.dumps(pipeline)), 'temp_name']

    client = Client()

    def test_process(self):
        dask.compute(do.process(self.pipeline))
        nb_files = len([name for name in os.listdir("test/data/output")])
        self.assertGreater(nb_files, 0)

    def test_serializePipeline(self):
        dask.compute(do.process(self.pipeline))
        do.serializePipeline(self.pipeline, "test/data/temp")
        nb_files = len([name for name in os.listdir("test/data/temp")])
        self.assertGreater(nb_files, 0)

    def test_process_delete_serialized_pipeline(self):
        do.serializePipeline(self.pipeline, "test/data/temp")
        nb_files_before = len([name for name in os.listdir("test/data/temp")])
        dask.compute(do.process(self.pipeline, "test/data/temp"))
        nb_files_after = len([name for name in os.listdir("test/data/temp")])
        self.assertEqual(nb_files_before - 1, nb_files_after)

    def test_process_serialized_pipelines(self):
        do.serializePipeline(self.pipeline, "test/data/temp")
        iterator = iter(self.pipeline[1])
        delayed = do.process_serialized_pipelines("test/data/temp", iterator)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_nodr_dir(self):
        iterator = iter([self.pipeline[1]])
        delayed = do.process_pipelines("test/data/output",
                                       "../test/data/pipeline.json",
                                       iterator,
                                       "../test/data/temp",
                                       False,
                                       False)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_nodr_singlefile(self):
        t = tile.Tile("test/data/input/echantillon_10pts.laz",
                      "../test/data/output", "../test/data/pipeline.json")
        iterator = iter([t])
        delayed = do.process_pipelines("test/data/output",
                                       "../test/data/pipeline.json",
                                       iterator,
                                       "../test/data/temp",
                                       False,
                                       True)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_dr_dir(self):
        iterator = iter([self.pipeline[1]])
        delayed = do.process_pipelines("test/data/output",
                                       "../test/data/pipeline.json",
                                       iterator,
                                       "../test/data/temp",
                                       True,
                                       False)
        self.assertGreater(len(delayed), 0)

    def test_process_pipelines_dr_singlefile(self):
        t = tile.Tile("test/data/input/echantillon_10pts.laz",
                      "../test/data/output", "../test/data/pipeline.json")
        iterator = iter([t])
        delayed = do.process_pipelines("test/data/output",
                                       "../test/data/pipeline.json",
                                       iterator,
                                       "../test/data/temp",
                                       True,
                                       True)

        self.assertGreater(len(delayed), 0)

    def test_splitCloud(self):
        result = do.splitCloud("test/data/input/echantillon_10pts.laz",
                               "../test/data/output",
                               "../test/data/pipeline.json",
                               (1000, 1000)
                               )
        self.assertIsNotNone(result)


class TestFileManager(unittest.TestCase):
    def test_getFiles_all(self):
        result = file_manager.getFiles("../test/data/input")
        self.assertGreater(len(list(next(result))), 3)

    def test_getFiles_nFiles(self):
        result = file_manager.getFiles("../test/data/input", 2)
        self.assertEqual(len(list(result)), 2)

    def test_getSerializedPipelines(self):
        result = file_manager.getSerializedPipelines("../test/data/temp")
        self.assertGreater(len(list(result)), 0)


class TestTile(unittest.TestCase):
    t = tile.Tile("test/data/input/echantillon_10pts.laz",
                  "../test/data/output",
                  "../test/data/pipeline.json",
                  bounds=bounds.Bounds(685019.31, 7047019.02, 685019.93, 7047019.98),
                  cloud_object=cloud.Cloud("../test/data/input/echantillon_10pts.laz"))

    def test_pipeline(self):
        result = self.t.pipeline(False)
        self.assertIsInstance(result[0], pdal.Pipeline)
        self.assertEqual(result[1], "temp__echantillon_10pts")

    def test_split(self):
        result = self.t.split(0.30, 0.30)
        self.assertEqual(len(list(result)), 12)


if __name__ == "__main__":
    unittest.main()