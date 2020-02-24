
import Foundation
import MIODB
import CMySQL

open class MIODBMySQL: MIODB {
    
    let defaultPort:Int32 = 5432
    let defaultUser = "root"
    let defaultDatabase = "public"
    
    var dbconnection:UnsafeMutablePointer<MYSQL>?
        
    open override func connect(){
        if port == nil { port = defaultPort }
        if user == nil { user = defaultUser }
        if database == nil { database = defaultDatabase }
        
        let conn = mysql_init(nil)
        
        if mysql_real_connect(dbconnection, host!, user!, password!, database!, 0, nil, 0) != nil {
            dbconnection = conn
        }
    }
    
    open override func connect(scheme:String?){
        connect()
        changeScheme(scheme)
    }
    
    open override func disconnect() {
        mysql_close(dbconnection)
        dbconnection = nil
    }
    
    open override func executeQueryString(_ query:String) throws -> [Any]{
        
        let status = mysql_query(dbconnection, query.cString(using: .utf8))
        if status == 0 {
            return []
        }

        let res = mysql_store_result(dbconnection); // Get the Result Set

        if res == nil {
            return []
        }
        
        var items:[Any] = []
        
        // Columns =
        var columns = [String]()
        var field:UnsafeMutablePointer<MYSQL_FIELD>? = mysql_fetch_field(res)
        while(field != nil ) {
            columns.append(String(cString: field!.pointee.name))
            field = mysql_fetch_field(res)
            //printf("%s ", field->name);
        }
        
        //let num_fields = mysql_num_fields(res);
        var row = mysql_fetch_row(res)
        while row != nil {
            var item = [String:Any]()
            for colIndex in 0..<columns.count {
                let colname = columns[colIndex]
                //let type = PQftype(res, col)
                let value = String(cString: row![colIndex]!)
                
                item[colname] = value //convert(value: value!, withType: type)
            }
            items.append(item)
            row = mysql_fetch_row(res)
        }
        
        mysql_free_result(res);
        
//            for row in 0..<mysql_fetch_row(res) {
//                var item = [String:Any]()
//                for col in 0..<PQnfields(res){
//                    if PQgetisnull(res, row, col) == 1 {continue}
//
//                    let colname = String(cString: PQfname(res, col))
//                    let type = PQftype(res, col)
//                    let value = PQgetvalue(res, row, col)
//
//                    item[colname] = convert(value: value!, withType: type)
//                }
//                items.append(item)
//            }
                            
        return items
    }
}
