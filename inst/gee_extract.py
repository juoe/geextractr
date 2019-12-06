import ee
ee.Initialize()
import json
import pandas

def gee_extract_py(fc_filepath, reducer_scale, tile_scale):

    global gee_object

    def fc2df(fc):
        features = fc.getInfo()['features']

        dictarr = []

        for f in features:
            attr = f['properties']
            dictarr.append(attr)

        return pandas.DataFrame(dictarr)

    with open(fc_filepath) as file:
       locations = json.load(file)

    fc_points = ee.FeatureCollection(locations['features'])

    if gee_object.name() == 'Image':
        ext = gee_object.reduceRegions(fc_points, reducer, reducer_scale, tileScale = tile_scale)
    elif gee_object.name() == 'ImageCollection':

        gee_object = gee_object.filterBounds(fc_points.geometry())

        def reduce_img(img):
            def set_date(f):
                return f.set('image_date', img.date().format("YYYY-MM-dd"))
            intersecting_points = fc_points.filterBounds(img.geometry())  #keep only features intersecting current image
            return img.reduceRegions(intersecting_points, reducer, reducer_scale, tileScale = tile_scale).map(set_date)

        ext = gee_object.map(reduce_img).flatten()

    return fc2df(ext)
