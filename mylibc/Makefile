CC = gcc
CFLAGS = -O2 -fPIC -Wall
LUADIR = /mnt/hgfs/shared/skynet/3rd/lua
TARGET = package.so
SKYNETDIR = /usr/maidong/skynet/skynet-src/

.PHONY : all clean

all : $(TARGET)

$(TARGET) : package_proto.c
	$(CC) $(CFLAGS) -shared -o $@ -I../.. -I$(LUADIR)  $^
	cp $(TARGET) /mnt/hgfs/shared/demo_svr/luaclib/

clean :
	rm -f $(TARGET)
