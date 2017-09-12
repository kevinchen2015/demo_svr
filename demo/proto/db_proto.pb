package proto.db;

enum msg_id {
    common_db_req_id = 1;
	common_db_rsp_id = 2;
}

message msg_struct {
	required uint32 id 		= 1;
	optional bytes body 	= 2;
}

message common_db_req {
    required uint32 session 	= 1;   
    required uint32 type    	= 2;
	required uint32 client_id 	= 3;
    required string cmd 		= 4;
	required string param   	= 5;
}

message common_db_rsp {
    required uint32 session 	= 1;   
    required uint32 type    	= 2; 
	required uint32 client_id 	= 3;	
    required uint32 result  	= 4; 

    optional string content 	= 5;    
}
