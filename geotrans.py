import osr , ogr 
         
class GeomTrans(object):
    def __init__(self, in_proj, out_proj):
        self.transform = None
        
        if in_proj :         
            self.inSpatialRef = osr.SpatialReference()
            self.inSpatialRef.SetFromUserInput(in_proj)
        else:
            return 
        
        if out_proj :        
            self.outSpatialRef = osr.SpatialReference()
            self.outSpatialRef.SetFromUserInput(out_proj)       
        else:
            return 
        
        if self.inSpatialRef.IsSame(self.outSpatialRef) == 0:
            self.transform = osr.CoordinateTransformation(self.inSpatialRef, self.outSpatialRef) 
                                
    def transform_point(self, point):
        if self.transform is None:
            return point        
        
        geom = ogr.Geometry(ogr.wkbPoint)
        geom.AddPoint(point[0], point[1])
        geom.Transform(self.transform) 
        
        return geom.GetX(), geom.GetY()
    
    def transform_points(self, points):
        return [ self.transform_point(point) for point in points ]
    
    def transform_geom(self, geometry): 
        if  geometry.find('{') >= 0 :           
            geom = ogr.CreateGeometryFromJson(geometry)
        else:
            geom = ogr.CreateGeometryFromWkt(geometry)
             
        if self.transform is not None:
            geom.Transform(self.transform) 
        
        return geom 
    
    def transform_wkt(self, geometry):
        return self.transform_geom(geometry).ExportToWkt()
        
    def transform_json(self, geometry): 
        return self.transform_geom(geometry).ExportToJson()