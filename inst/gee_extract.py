import ee
ee.Initialize()
import json
import pandas

def gee_extract_py(fc_filepath, reducer_scale):

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

    if image.name() == 'Image':
        ext = image.reduceRegions(fc_points, reducer, reducer_scale)
    elif image.name() == 'ImageCollection':
        def reduce_img(img):
            def set_date(f):
                return f.set('image_date', img.date().format("YYYY-MM-dd"))
            return img.reduceRegions(fc_points, reducer, reducer_scale).map(set_date)

        ext = image.map(reduce_img).flatten()

    return fc2df(ext)
