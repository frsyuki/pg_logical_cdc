pg_config := pg_config
PG_CONFIG_FLAGS = -I$(shell $(pg_config) --includedir) -L$(shell $(pg_config) --libdir)
CFLAGS := -Wall -O2
LDFLAGS := -lpq
CC := cc

pg_logical_stream:
	$(CC) $(PG_CONFIG_FLAGS) $(CFLAGS) $(LDFLAGS) pg_logical_stream.c -lpq -o $@

clean:
	rm -f pg_logical_stream

