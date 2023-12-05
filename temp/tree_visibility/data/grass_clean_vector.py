import grass.script as gs

def vector_info(base_name):
    """
    Get information about a vector map.
    """
    
    # Run the v.info command
    gs.run_command('v.info', map=base_name) 
    
    # RUn v.info -c map=oslo_bldg 
    gs.run_command('v.info', flags='c', map=base_name)
    return None

def snap_vertices(base_name, input_map, threshold_snap):
    """
    Snap vertices of input vector if objects are within threshold distance.
    
    Args:
        input_map (str): name of input vector map
        threshold (float): distance to snap vertices 
            (0.0 = no snapping, 0.1 = 10 cm, 1.0 = 1 m, etc.)
    
    Returns:
        output_map: layer in mapset of input_map with snapped vertices
    """
    
    output_map = 'snap_' + base_name
    
    # Run the v.clean command
    gs.run_command('v.clean', input=input_map, output=output_map, tool='snap', threshold=threshold_snap)
    return output_map
    
def dissolve_objects(base_name, input_map,column):
    """
    Dissolve objects that share a common boundary 
    AND have the same value in the column.
    
    Args: 
    """
    output_map = 'dissolve_' + base_name
    
    # Run the v.dissolve command
    gs.run_command('v.dissolve', input=input_map, output=output_map, column=column)
    return output_map

def clean_topology(base_name, input_map, threshold_area):
    """
    Remove lines that have no category in a vector map.
    """
    output_map = 'top_clean_' + base_name
    
    # remove duplicates 
    gs.run_command('v.clean', input=input_map, output=output_map, tool='rmdupl')
    
    # remove danling lines 
    gs.run_command('v.clean', input=input_map, output=output_map, tool='bpol')
    
    # remove small areas (less than 1m2)
    gs.run_command('v.clean', input=input_map, output=output_map, tool='rmarea', threshold=threshold_area)

    return output_map

def report_cleaning(base_name, mapset):
    """
    Report on cleaning of topology.
    """
    
    # report on snapping
    map = 'snap_' + base_name + '@' + mapset
    gs.run_command('v.info', map=map)
    
    # report on dissolving
    map = 'dissolve_' + base_name + '@' + mapset
    gs.run_command('v.info', map=map)
    
    # report on cleaning topology
    map = 'top_clean_' + base_name + '@' + mapset
    gs.run_command('v.info', map=map)
    
    return None

def export_to_geojson(base_name, input_map, output_path):
    """
    Export vector map to geojson.
    """
    filename = 'cl_' + base_name
    output= output_path + filename + '.geojson'
    gs.run_command('v.out.ogr', input=input_map, output=output, format='GeoJSON')
    return None


def polygon_workflow(input_map, base_name, mapset, output_path, threshold_snap, dissolve_column, threshold_area):

    # snap vertices
    snap_map = snap_vertices(base_name, input_map, threshold_snap)
    
    # dissolve objects
    dissolve_map = dissolve_objects(base_name, snap_map, dissolve_column)
    
    # clean topology
    clean_map = clean_topology(base_name, dissolve_map, threshold_area)

    # report on cleaning
    report_cleaning(base_name, mapset)
    
    clean_map = 'top_clean_' + base_name + '@' + mapset
    # export to geojson
    export_to_geojson(base_name, clean_map, output_path)
    
    return None

if __name__ == '__main__':
    
    # paths
    input_map = 'oslo_bldg'
    base_name = 'oslo_bldg'
    mapset = 'oslo_split_data'
    
    output_path = r"home/NINA.NO/willeke.acampo/Mounts/P-Prosjekter2/152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst/TEMP/oslo/grass_clean/"
    
    polygon_workflow(
        input_map=input_map,
        base_name=input_map,
        mapset=mapset,
        threshold_snap=0.001,
        dissolve_column='bygningstype',
        threshold_area=1.0
    )
   
# Grass commands
# import
"""
v.import input=/home/NINA.NO/willeke.acampo/Mounts/P-Prosjekter2/152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst/TEMP/oslo/08_reporting/oslo_study_area.geojson 
layer=oslo_study_area output=study_area
"""

# snap vertices     
"""
v.clean input=bldg output=snap_bldg tool=snap threshold=0.1
"""

# dissolve objects
# gs.run_command('v.dissolve', input=input_map, output=output_map, column=column)
# bygningstype
# grontomradeklasse
# open_space

# v.db.update map=snap_open_space@oslo_split_data columns=open_space value=åpent område
# v.db.addcolumn map=snap_open_space columns=open_space                           

"""
v.dissolve input=snap_bldg output=dissolve_bldg column=bygningstype
"""

# clean topology
# gs.run_command('v.clean', input=input_map, output=output_map, tool='rmdupl')
"""
v.clean input=dissolve_bldg output=top_clean_bldg tool=rmdupl
"""

# export to geojson
# gs.run_command('v.out.ogr', input=input_map, output=output, format='GeoJSON')
# best to do from gui
"""
v.out.ogr input=top_clean_bldg 
output=/home/NINA.NO/willeke.acampo/Mounts/P-Prosjekter2/152022_itree_eco_ifront_synliggjore_trars_rolle_i_okosyst/TEMP/oslo/grass_clean/cl_MUNICIPALITY_bldg.geojson format=GeoJSON
"""