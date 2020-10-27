# -*- coding: utf-8 -*-
"""
Created on Sun Oct 25 21:03:21 2020

@author: apee461
"""

from arcgis import GIS
gis = GIS ("https://apee461.cc4soe.cloud.edu.au/portal","admin","bobby2Tables")

content = gis.content.search("golf_courses",item_type="Feature Layer")
content[0]

map1 = gis.map()
map1.extent = content[0].extent
map1.add_layer(content[0])
map1

#%%
from arcgis.features import FeatureLayerCollection
from arcgis.features.use_proximity import create_buffers

golf_courses = content[0]
buffer_golf = create_buffers(golf_courses,
                             dissolve_type='Dissolve',
                             distances=[0.5],
                             ring_type='Rings',
                             units='Miles',
                             output_name="GolfBuffer")

map1.add_layer(buffer_golf)
map1