#ifndef EXTENDEDPROPERTY_H
#define EXTENDEDPROPERTY_H

extern void delete_extended_property(int nkeys,
                                     int16 db_id,
                                     const char *schema_name,
                                     const char *major_name,
                                     const char *minor_name,
                                     const char *type);
extern void update_extended_property(int nkeys,
                                     int16 db_id,
                                     const char *schema_name,
                                     const char *major_name,
                                     const char *minor_name,
                                     const char *type,
                                     int attnum,
                                     const char *new_value);
extern void babelfish_exec_extendedproperty(PG_FUNCTION_ARGS,
                                            const char *procedure_name);

#endif