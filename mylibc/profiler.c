#include <lua.h>
#include <lualib.h>
#include <lauxlib.h>
#include <string.h>
#include <time.h>
#include "uthash.h"


struct profiler_log {
	int linedefined;
	char source[LUA_IDSIZE+16];
	
	float  start;			
	float  cost_max;		
	float  cost_total;
	int    counter;
	int    deep;

	//char   name[LUA_IDSIZE];
	
	UT_hash_handle hh;  
};

struct profiler_count {
	int total;
	int index;

	int mode;		//模式1：流式输出,counter 不生效，save也不生效,       2: hash 统计输出，会包含寻找hash的开销
	int out_cb;		//模式1：流式输出回调
	float start;
	float end;
};

static struct profiler_log* root; 
const char* title = "[lua profiler2.c]";
static int is_start = 0;

static void 
print_out(struct profiler_count* p,lua_State* L,char* msg){

	if(p->out_cb != 0)
	{
		lua_rawgeti(L, LUA_REGISTRYINDEX, p->out_cb);
		lua_pushstring(L,title);
		lua_pushstring(L, msg);
		lua_call(L, 2, 0);
	}
	else
	{
		printf("\r\n %s %s \r\n",title,msg);
	}
	
}

static void
profiler_my_hook(lua_State *L, lua_Debug *ar){
	struct profiler_count *p = NULL;
	struct profiler_log * log = NULL;
	struct profiler_log* t = NULL;
	float start = 0, end = 0, dt = 0;
	char msg[2048] = {0x00};
	char temp[1024] = {0x00};
	int index = 0;

	if(!ar || !L) return;
	
	
	if(is_start == 0)return;
	/*
	if (lua_rawgetp(L, LUA_REGISTRYINDEX, L) != LUA_TUSERDATA) {
		lua_pop(L, 1);
		return;
	}
	*/
	
	lua_getglobal(L,"myprofiler_data");
	p = (struct profiler_count *)lua_touserdata(L, -1);
	lua_pop(L, 1);
	log = (struct profiler_log *)(p+1);

	if(p->mode == 1)
	{
		if (lua_getinfo(L, "S", ar) != 0) {
			if(ar->event == 0)
			{
				start = (float)((long)clock())/((long)CLOCKS_PER_SEC);
				//if(ar->name)
				//	sprintf(msg," hook begin call ,name:%s,start time:%f , lua: %s ,line:%d  ",ar->name,start,ar->short_src,ar->linedefined);
				//else
					sprintf(msg," hook begin call ,start time:%f , lua: %s ,line:%d  ",start,ar->short_src,ar->linedefined);
				print_out(p,L,msg);
			}
			else if(ar->event == 1)
			{
				end = (float)((long)clock())/((long)CLOCKS_PER_SEC);
				//if(ar->name)
				//	sprintf(msg," hook end call ,name:%s,end time:%f , lua: %s ,line:%d  ",ar->name,end,ar->short_src,ar->linedefined);
				//else
					sprintf(msg," hook end call ,end time:%f , lua: %s ,line:%d  ",end,ar->short_src,ar->linedefined);
				print_out(p,L,msg);
			}
			else
			{
				//if(ar->name)
				//	sprintf(msg," hook event,event:%d,name:%s, lua: %s ,line:%d  ",ar->name,ar->event,ar->short_src,ar->linedefined);
				//else
					sprintf(msg," hook event,event:%d,lua: %s ,line:%d  ",ar->event,ar->short_src,ar->linedefined);
				print_out(p,L,msg);
			}
		} 
		else
		{
			sprintf(msg," call function ,event:%d , [unknow] !",ar->event);
			print_out(p,L,msg);
		}
	}
	else if(p->mode == 2)
	{
		if (lua_getinfo(L, "S", ar) != 0) {
			if(ar->event == 0)
			{
				t = (struct profiler_log *)0;
				sprintf(temp,"%s_(%d)",ar->short_src,ar->linedefined);
				HASH_FIND_STR(root,temp,t);	
				if(!t)
				{
					index = ++(p->index);
					if(index >= p->total)
					{
						//printf(" need mor index!!!! ");
						return;
					}
					t = &(log[index]);
					t->linedefined = ar->linedefined;
					strcpy(t->source, temp);
					//strcpy(t->name,ar->name);
					t->counter = 0;
					t->cost_max = 0.0f;
					t->cost_total = 0.0f;
					t->start = 0.0f;
					t->deep = 0;
					HASH_ADD_STR(root,source,t);
				}
				if(t->deep == 0)
				{
					t->start = (float)((long)clock())/((long)CLOCKS_PER_SEC);
				}
				++(t->deep);
				//printf("call line:%d +deep:%d",ar->linedefined,t->deep);
				++(t->counter);  
			}
			else if(ar->event == 1)
			{
				t = (struct profiler_log *)0;
				sprintf(temp,"%s_(%d)",ar->short_src,ar->linedefined);
				HASH_FIND_STR(root,temp,t);		
				if(t){
					//递归调用，只记录最外部的一次调用时间，计数还是会叠加
					--(t->deep);
					//printf("ret line:%d -deep:%d",ar->linedefined,t->deep);
					if(t->deep == 0)
					{
						float end = (float)((long)clock())/((long)CLOCKS_PER_SEC);
						float dt = end-t->start;
						if(dt > t->cost_max)
						{
							t->cost_max = dt;
						}
						t->cost_total += dt;
					}
				}
			}
			else
			{
				sprintf(msg," hook event,event:%d , lua: %s ,line:%d  ",ar->event,ar->short_src,ar->linedefined);
				print_out(p,L,msg);
			}
		} else {
			sprintf(msg," call function ,event:%d , [unknow] !",ar->event);
			print_out(p,L,msg);
		}
	}
	else
	{
		sprintf(msg," call function ,event:%d , [unknow] !",ar->event);
		print_out(p,L," mode error!");
	}
}

static int
lstart(lua_State *L) {

	
	lua_State *cL = L;
	int count = 0;
	int out_cb = 0;
	int args = 0;
	int size = 0;
	struct profiler_count *p = NULL;
	if(is_start > 0)return 0;
	if (lua_isthread(L, 1))
	{
		cL = lua_tothread(L, 1);
		args = 1;
	}
	count = luaL_optinteger(L, args+1, 1000);
	mode = luaL_optinteger(L, args+2, 2);
	if(0 == lua_isnil(L,args+3))
	{
		out_cb = luaL_ref(L, LUA_REGISTRYINDEX);   //must be top of stack!
	}
	size = sizeof(struct profiler_count) + count * sizeof(struct profiler_log);
	
	p = (struct profiler_count *)malloc(size);//lua_newuserdata(L,size);
	lua_pushlightuserdata(L,p);
	
	memset(p,0x00,size);
	// root = (struct profiler_log*)0;
	p->total = count;
	p->index = 0;
	p->mode = mode;
	p->out_cb = out_cb;
	
	/*
	lua_pushvalue(L, -1);
	lua_rawsetp(L, LUA_REGISTRYINDEX, cL);
	*/
	
	lua_pushlightuserdata(L,p);
	lua_setglobal(L,"myprofiler_data");
	
	is_start = 1;
	lua_sethook(cL, profiler_my_hook, LUA_MASKCALL | LUA_MASKRET , 0);
	p->start = (float)((long)clock())/((long)CLOCKS_PER_SEC);
	return 0;
}

static int
lstop(lua_State *L) {
	lua_State *cL = L;
	struct profiler_count * p = NULL;
	struct profiler_log *log = NULL;
	if(0==is_start)return 0;
	if (lua_isthread(L, 1))
	{
		cL = lua_tothread(L, 1);
	}
	
	
	/*
	if (lua_rawgetp(L, LUA_REGISTRYINDEX, L) == LUA_TUSERDATA) {
		struct profiler_count * p = lua_touserdata(L, -1);
		lua_pop(L, 1);
		struct profiler_log * log = (struct profiler_log *)(p+1);
		if(p->out_cb != 0)
		{
			luaL_unref(L, LUA_REGISTRYINDEX,p->out_cb);
			p->out_cb = 0;
		}
	}
	if (lua_rawgetp(L, LUA_REGISTRYINDEX, cL) != LUA_TNIL) {
		lua_pushnil(L);
		lua_rawsetp(L, LUA_REGISTRYINDEX, cL);
		lua_sethook(cL, NULL, 0, 0);
	} else {
		return luaL_error(L, "thread profiler not begin");
	}
	*/
	
	
	lua_getglobal(L,"myprofiler_data");
	p = (struct profiler_count *)lua_touserdata(L, -1);
	lua_pop(L, 1);
	log = (struct profiler_log *)(p+1);
	p->end = (float)((long)clock())/((long)CLOCKS_PER_SEC);
	if(p->out_cb != 0)
	{
		luaL_unref(L, LUA_REGISTRYINDEX,p->out_cb);
		p->out_cb = 0;
	}
	is_start = 0;
	lua_sethook(cL, NULL, 0, 0);
	
	free(p);
	
	return 0;
}

static int
lsave(lua_State *L) {
	lua_State *cL = L;
	struct profiler_count *p = NULL;
	struct profiler_log *log = NULL;
	int i = 0;
	int n = 0;
	FILE *fp = NULL;
	char msg[2048] = {0x00};

	if(0==is_start)return 0;
	if (lua_isthread(L, 1))
	{
		cL = lua_tothread(L, 1);
	}
	const char* path = lua_tostring(L,-1);
	/*
	if (lua_rawgetp(L, LUA_REGISTRYINDEX, cL) != LUA_TUSERDATA) {
		return luaL_error(L, "thread profiler not begin");
	}
	*/
	
	lua_getglobal(L,"myprofiler_data");
	p = (struct profiler_count *)lua_touserdata(L, -1);
	log = (struct profiler_log *)(p+1);
	
	p->end = (float)((long)clock())/((long)CLOCKS_PER_SEC);
	(p->index > p->total) ? p->total : p->index;

	fp = fopen(path,"wb");
	if(fp)
	{
		{
			char msg[1024] = {0x00};
			float dt = p->end-p->start;
			sprintf(msg," lua profiler info start:%f,end:%f ,dt:%f \n",
					p->start,p->end,dt);
				fwrite(msg,1,strlen(msg)+1,fp);
		}
		for(i=0;i<n;++i)
		{
			//if(log[i].cost_total > 0.0001f)
			{	
				//sprintf(msg," cost_total:%f, cost_max:%f, call_count:%d ,deep:%d, name:%s, lua_source:%s ,lua_line:%d\n",
				//	log[i].cost_total,log[i].cost_max,log[i].counter,log[i].deep,log[i].name,log[i].source,log[i].linedefined);
				
				sprintf(msg," cost_total:%f, cost_max:%f, call_count:%d ,deep:%d, lua_source:%s ,lua_line:%d\n",
					log[i].cost_total,log[i].cost_max,log[i].counter,log[i].deep,log[i].source,log[i].linedefined);
				fwrite(msg,1,strlen(msg)+1,fp);
			}
		}
		fflush(fp);
		fclose(fp);
	}
	else{
		printf("\r\n save file error! path:%s",path);
	}
}
static int
linfo(lua_State *L) {
	lua_State *cL = L;
	struct profiler_count *p = NULL;
	struct profiler_log *log = NULL;
	int n = 0;
	int i = 0;
	if(0==is_start)return 0;
	if (lua_isthread(L, 1))
	{
		cL = lua_tothread(L, 1);
	}
	
	/*
	if (lua_rawgetp(L, LUA_REGISTRYINDEX, cL) != LUA_TUSERDATA) {
		return luaL_error(L, "thread profiler not begin");
	}
	*/
	lua_getglobal(L,"myprofiler_data");
	p = (struct profiler_count *)lua_touserdata(L, -1);
	log = (struct profiler_log *)(p+1);
	p->end = (float)((long)clock())/((long)CLOCKS_PER_SEC);
	n = (p->index > p->total) ? p->total : p->index;
	/*
	for (i=0;i<n;i++) {
		//save info...
		printf(" cost_total:%f,cost_max:%f,call_counter:%d,src:%s,line:%d,deep:%d",
		log[i].cost_total,log[i].cost_max,log[i].counter,log[i].source,log[i].linedefined,log[i].deep);
	}
	*/

	lua_newtable(L);
	for(i=0;i<n;++i)
	{
		lua_newtable(L);
		{
			lua_pushstring(L,"cost_total");
			lua_pushnumber(L, log[i].cost_total);
			lua_settable(L,-3);

			lua_pushstring(L,"cost_max");
			lua_pushnumber(L, log[i].cost_max);
			lua_settable(L,-3);

			lua_pushstring(L,"call_count");
			lua_pushinteger(L, log[i].counter);
			lua_settable(L,-3);
			
			//lua_pushstring(L,"name");
			//lua_pushstring(L, log[i].name);
			//lua_settable(L,-3);

			lua_pushstring(L,"lua_source");
			lua_pushstring(L, log[i].source);
			lua_settable(L,-3);

			lua_pushstring(L,"lua_line");
			lua_pushinteger(L, log[i].linedefined);
			lua_settable(L,-3);

			lua_pushstring(L,"deep");
			lua_pushinteger(L, log[i].deep);
			lua_settable(L,-3);
		}
		lua_rawseti(L,-2,i+1);
	}
	float dt = p->end-p->start;
	lua_pushinteger(L, n);
	lua_pushnumber(L,p->start);
	lua_pushnumber(L,p->end);
	lua_pushnumber(L,dt);
	return 5;
}

/*
LUALIB_API int
luaopen_profiler(lua_State *L) {
	//luaL_checkversion(L);
	
	luaL_Reg l[] = {
		{ "myprofiler_start", lstart },
		{ "myprofiler_info", linfo },		//info 调用要在stop之前
		{ "myprofiler_stop", lstop },

		{ NULL, NULL },
	};
	luaL_newlib(L, l);
	
	
	return 1;
}
*/

static const struct luaL_reg myprofiler_func [] = 
{
  	{ "_start", lstart },
	{ "_info", linfo },		//info 调用要在stop之前
	{ "_save", lsave },
	{ "_stop", lstop },
    {NULL, NULL}
};

LUALIB_API int luaopen_profiler (lua_State *L)
{

	luaL_register(L, "myprofiler", myprofiler_func);
	
	//lua_pushcfunction(L,lstart);
	//lua_pushcfunction(L,linfo);
	//lua_pushcfunction(L,lstop);
	//lua_pushcfunction(L,lsave);
	
    return 1;
} 