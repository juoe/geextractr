def landsat_indices (landsat, indices):
    
    # helper functions to add spectral indices to collections

    def add_NDVI (image):
        ndvi = image.normalizedDifference(['NIR', 'R']).rename('NDVI')
        return image.addBands(ndvi)

    def add_NBR (image):
        nbr = image.normalizedDifference(['NIR', 'SWIR2']).rename('NBR')
        return image.addBands(nbr)

    def add_NBR2 (image):
        nbr2 = image.normalizedDifference(['SWIR1', 'SWIR2']).rename('NBR2')
        return image.addBands(nbr2)

    def add_NDMI (image):
        ndvi = image.normalizedDifference(['NIR', 'SWIR1']).rename('NDMI')
        return image.addBands(ndvi)

    def add_EVI (image):
        evi = image.expression('2.5 * ((NIR - R) / (NIR + 6 * R - 7.5 * B + 1))', {'NIR': image.select('NIR'),'R': image.select('R'),'B': image.select('B')}).rename('EVI')
        return image.addBands(evi)

    def add_SAVI (image): 
        savi = image.expression('((NIR - R) / (NIR + R + 0.5 )) * (1.5)', {'NIR': image.select('NIR'),'R': image.select('R')}).rename('SAVI')
        return image.addBands(savi)
    
    def add_MSAVI (image): 
        msavi = image.expression('(2 * NIR + 1 - sqrt(pow((2 * NIR + 1), 2) - 8 * (NIR - R)) ) / 2', {'NIR': image.select('NIR'),'R': image.select('R')}).rename('MSAVI')
        return image.addBands(msavi)

    def add_TC (image):
        
        img = image.select(['B', 'G', 'R', 'NIR', 'SWIR1', 'SWIR2'])
        
        # coefficients for Landsat surface reflectance (Crist 1985)
        brightness_c= ee.Image([0.2043, 0.4158, 0.5524, 0.5741, 0.3124, 0.2303]);
        greenness_c= ee.Image([-0.1603, 0.2819, -0.4934, 0.7940, -0.0002, -0.1446]);
        wetness_c= ee.Image([0.0315,  0.2021,  0.3102,  0.1594, -0.6806, -0.6109]);

        brightness = img.multiply(brightness_c)
        greenness = img.multiply(greenness_c)
        wetness = img.multiply(wetness_c)

        brightness = brightness.reduce(ee.call('Reducer.sum'))
        greenness = greenness.reduce(ee.call('Reducer.sum'))
        wetness = wetness.reduce(ee.call('Reducer.sum'))

        tasseled_cap = ee.Image(brightness).addBands(greenness).addBands(wetness).rename(['brightness','greenness','wetness'])

        return image.addBands(tasseled_cap)


    out = landsat.map(add_NDVI).map(add_NBR).map(add_NBR2).map(add_NDMI).map(add_EVI).map(add_SAVI).map(add_MSAVI).map(add_TC).select(indices)

    return out