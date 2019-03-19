import ee
ee.Initialize()
import json
import pandas


def gee_match_extract_py(fc_filepath, reducer_scale, prefilter, prefilter_buffer):

    # helper function to convert feature collection to pandas data frame
    def fc2df(fc):
        features = fc.getInfo()['features']

        dictarr = []

        for f in features:
            attr = f['properties']
            dictarr.append(attr)

        return pandas.DataFrame(dictarr)

    # open fc from disk
    with open(fc_filepath) as file:
       locations = json.load(file)

    fc_points = ee.FeatureCollection(locations['features'])

    # convert dates into correct formats
    def correct_date(feature):
        corrected_date = ee.String(feature.get('date')).replace("/", "-", 'g')
        return feature.set('date', corrected_date)

    fc_points = fc_points.map(correct_date)

    # helper functions to perform temporal matching
    def add_time_diff(image, date):
        time_diff = ee.Date(date).difference(image.date(), 'day').abs();
        return image.set('time_diff', time_diff)

    def find_closest(ic, feature, date):
        ic_filter = ic.filterBounds(feature.geometry())  # only include images intersecting current location

        if prefilter:
            ic_filter = ic_filter.filterDate(ee.Date(date).advance(-prefilter_buffer, 'day'),
                                             ee.Date(date).advance(prefilter_buffer, 'day'))

        def mapfun(ic_filter):
            return add_time_diff(ic_filter, date)

        ic_diff = ee.ImageCollection(ic_filter).map(mapfun)
        return ee.Image(ic_diff.sort('time_diff').first())

    def extract_layers(fc_points, ic):

        def reduce_feature(feature):
            date_millis = ee.Date(feature.get('date')).millis()
            matched_image = find_closest(ic, feature, date_millis)


            out = feature.set(matched_image.reduceRegion(
                reducer = reducer,
                geometry = feature.geometry(),
                scale = reducer_scale)).set(
                'image_date', matched_image.date().format("YYYY-MM-dd")
            )

            return out


        return fc_points.map(reduce_feature)

    ext = extract_layers(fc_points, collection)

    return fc2df(ext)
