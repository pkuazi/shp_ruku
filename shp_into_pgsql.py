'''
Created on Dec 30, 2019

@author: root
'''
import pgsql
import os
import shapely, fiona
from asn1crypto._ffi import null
import datetime
from osgeo import ogr,osr
import json
from geotrans import GeomTrans

# data_root = "/mnt/gscloud/LANDSAT"
shp_path = "/mnt/win/phd/samples/pd_1995"
shp_file = os.path.join(shp_path,'PD_1995_120035.shp')
# geojson=os.path.join(shp_path, 'pd_1995_120035.geojson')

pg_src = pgsql.Pgsql("10.0.85.20", "postgres", "", "mark")
# def parse_shp_to_geojson(shpfile):
#     return geojson
import uuid
def labels_into_pgsql(geom, labelid,proj):
    sampletype =0;
    imageres=30;
    imageid="LC81200342015133LGN00";
    taskid = 11;
    tagid=labelid;
#     insert_sql = """INSERT INTO public.samples(
#             img, gid, userid, label, geom, ctime, taskid, name, abstract, 
#             sampleid, data, year, id, projection, type, source)
#     VALUES (0, null, 0, %s, %s, %s,  %s, %s, null, %s, null, 2015, %s, %s, %s, 1);  """
    mtime = datetime.datetime.now()
    guid=str(uuid.uuid4()).replace('-','')
    insert_sql = """INSERT INTO sample( guid, geom, labelid, tagid, taskid, imageid, imageres, sampletype,mtime, projection)
    VALUES (%s,%s, %s, %s, %s, %s, %s,%s,%s,%s);    """
    update_sql = """UPDATE sample SET  guid=%s, geom=%s, labelid=%s, tagid=%s, taskid=%s, imageid=%s, imageres=%s, sampletype=%s, mtime=%s, projection=%s, imagetime=%s"""
    
    sql = "select * from sample where taskid='%s' " % (taskid)
    datas = pg_src.getAll(sql)

    if len(datas) == 0:
        pg_src.update(insert_sql, (guid,geom, labelid, labelid,taskid, imageid,30,0,mtime,proj))
        print("insert ", labelid)
    else:
        pg_src.update(update_sql, (guid,geom, labelid, labelid,taskid, imageid,30,0,mtime,proj))
        print("update ", labelid)
                

if __name__ == '__main__':   
    with fiona.open(shp_file, 'r') as inp:
        projection = inp.crs_wkt
        
        for f in inp:
            geojson = json.dumps(f['geometry'])
            geom = ogr.CreateGeometryFromJson(geojson)
                 
            outSpatialRef = osr.SpatialReference()
            outSpatialRef.ImportFromEPSG(4326) 
                
            inSpatialRef = osr.SpatialReference()
            inSpatialRef.ImportFromWkt(projection)
        
            transform = osr.CoordinateTransformation(inSpatialRef, outSpatialRef) 
            trans_state = geom.Transform(transform)
            
            if trans_state==0:
#             geom_wgs = GeomTrans(projection, 'EPSG:4326').transform_geom(geom)
                wkt = geom.ExportToWkt()
                
                type = f['geometry']['type']
    #             data = f['properties']['data']
                label = f['properties']['class_id']
    #             name = label
    #             type = f['properties']['type']
    #             source=1
                if label==4 or label==5:
                    labels_into_pgsql(wkt,label,'EPSG:4326')
            


