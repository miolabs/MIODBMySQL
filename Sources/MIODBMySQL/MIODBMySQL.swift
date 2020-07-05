
import Foundation
import MIODB
import CMySQL

open class MIODBMySQL: MIODB {
    
    let defaultPort:Int32 = 5432
    let defaultUser = "root"
    let defaultDatabase = "public"
    
    var dbconnection:UnsafeMutablePointer<MYSQL>?
    
    public var includeTableNamesInColumnNames = false
        
    open override func connect(){
        if port == nil { port = defaultPort }
        if user == nil { user = defaultUser }
        if database == nil { database = defaultDatabase }
        
        let conn = mysql_init(nil)
        
        if mysql_real_connect(conn, host!, user!, password!, database!, 0, nil, 0) != nil {
            dbconnection = conn
        }
    }
    
    open override func disconnect() {
        mysql_close(dbconnection)
        dbconnection = nil
    }
    
    @discardableResult open override func executeQueryString(_ query:String) throws -> [[String : Any]]{
        
        if isInsideTransaction {
            pushQueryString(query)
            return []
        }
        
        let status = mysql_query(dbconnection, query.cString(using: .utf8))
        if status != 0 {
            return []
        }

        let res = mysql_store_result(dbconnection); // Get the Result Set

        if res == nil {
            return []
        }
        
        var items:[Any] = []
        
        // Columns =
        var columns = [String]()
        var types = [enum_field_types]()
        var field:UnsafeMutablePointer<MYSQL_FIELD>? = mysql_fetch_field(res)
        while(field != nil ) {
            
            var fieldName = String(cString: field!.pointee.name)
            if includeTableNamesInColumnNames == true {
                fieldName = String(cString: field!.pointee.table) + "." + fieldName
            }
            columns.append(fieldName)
            types.append(field!.pointee.type)
            field = mysql_fetch_field(res)
            //printf("%s ", field->name);
        }
        
        //let num_fields = mysql_num_fields(res);
        var row = mysql_fetch_row(res)
        var lengths = mysql_fetch_lengths(res)
        while row != nil {
            var item = [String:Any]()
            for colIndex in 0..<columns.count {
                let colname = columns[colIndex]
                let type = types[colIndex]
                
                if let value = row![colIndex] {
                    let length = lengths![colIndex]
                    item[colname] = convert(value: value, withType: type, length: length)
                }
            }
            items.append(item)
            row = mysql_fetch_row(res)
            lengths = mysql_fetch_lengths(res)
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
                            
        return items as! [[String : Any]]
    }
    
    func convert(value:UnsafePointer<Int8>, withType type: enum_field_types, length: UInt) -> Any? {
        
        switch type {
        case MYSQL_TYPE_DECIMAL, MYSQL_TYPE_NEWDECIMAL:
            return Decimal(string: String(cString: value))
        case MYSQL_TYPE_TINY, MYSQL_TYPE_SHORT, MYSQL_TYPE_LONG, MYSQL_TYPE_LONGLONG, MYSQL_TYPE_INT24:
            return Int(String(cString: value))
        case MYSQL_TYPE_FLOAT:
            return Float(String(cString: value))
        case MYSQL_TYPE_DOUBLE:
            return Double(String(cString: value))
        case MYSQL_TYPE_NULL:
            return nil
        case MYSQL_TYPE_TIMESTAMP:
            return Date(timeIntervalSince1970: TimeInterval(String(cString: value)) ?? 0)
//        case MYSQL_TYPE_DATE:
//        case MYSQL_TYPE_TIME:
//        case MYSQL_TYPE_DATETIME:
//        case MYSQL_TYPE_YEAR:
//        case MYSQL_TYPE_NEWDATE:
//        case MYSQL_TYPE_VARCHAR:
//        case MYSQL_TYPE_BIT:
//        case MYSQL_TYPE_SET:
        case MYSQL_TYPE_TINY_BLOB, MYSQL_TYPE_MEDIUM_BLOB, MYSQL_TYPE_LONG_BLOB, MYSQL_TYPE_BLOB, MYSQL_TYPE_STRING:
            return Data(bytes: value, count: Int(length))
        case MYSQL_TYPE_VAR_STRING:
            return String(cString: value)
//        case MYSQL_TYPE_GEOMETRY:
        default:
            return String(cString: value)
        }
    }
}

/*
 enum enum_field_types {
   MYSQL_TYPE_DECIMAL   0
 , MYSQL_TYPE_TINY      1
 , MYSQL_TYPE_SHORT     2
 , MYSQL_TYPE_LONG      3
 , MYSQL_TYPE_FLOAT     4
 , MYSQL_TYPE_DOUBLE    5
 , MYSQL_TYPE_NULL      6
 , MYSQL_TYPE_TIMESTAMP 7
 , MYSQL_TYPE_LONGLONG  8
 , MYSQL_TYPE_INT24     9
 , MYSQL_TYPE_DATE
 , MYSQL_TYPE_TIME
 , MYSQL_TYPE_DATETIME
 , MYSQL_TYPE_YEAR
 , MYSQL_TYPE_NEWDATE
 , MYSQL_TYPE_VARCHAR
 , MYSQL_TYPE_BIT
 ,
                         /*
                           mysql-5.6 compatibility temporal types.
                           They're only used internally for reading RBR
                           mysql-5.6 binary log events and mysql-5.6 frm files.
                           They're never sent to the client.
                         */
                         MYSQL_TYPE_TIMESTAMP2,
                         MYSQL_TYPE_DATETIME2,
                         MYSQL_TYPE_TIME2,
                         
                         MYSQL_TYPE_NEWDECIMAL=246,
             MYSQL_TYPE_ENUM=247,
             MYSQL_TYPE_SET=248,
             MYSQL_TYPE_TINY_BLOB=249,
             MYSQL_TYPE_MEDIUM_BLOB=250,
             MYSQL_TYPE_LONG_BLOB=251,
             MYSQL_TYPE_BLOB=252,
             MYSQL_TYPE_VAR_STRING=253,
             MYSQL_TYPE_STRING=254,
             MYSQL_TYPE_GEOMETRY=255

 };
 */

extension MDBQuery {
    
    public func equal(field:String, mysqlHexString:String?) -> MDBQuery {
        
        var valueString = ""
        
        if let mysqlHexString = mysqlHexString {
            valueString = "UNHEX('\(mysqlHexString)')"
        } else {
            valueString = "NULL"
        }

        items.append("\(field) = \(valueString)")
        return self
    }
}
