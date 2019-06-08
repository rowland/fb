 /*
  * fb.c
  * A module to access the Firebird database from Ruby.
  * Fork of interbase.c to fb.c by Brent Rowland.
  * All changes, improvements and associated bugs Copyright (C) 2006 Brent Rowland and Target Training International.
  * License to all changes, improvements and bugs is granted under the same terms as the original and/or
  * the Ruby license, whichever is most applicable.
  * Based on interbase.c
  *
  *                               Copyright (C) 1999 by NaCl inc.
  *                               Copyright (C) 1997,1998 by RIOS Corporation
  *
  * Permission to use, copy, modify, and distribute this software and its
  * documentation for any purpose and without fee is hereby granted, provided
  * that the above copyright notice appear in all copies.
  * RIOS Corporation makes no representations about the suitability of
  * this software for any purpose.  It is provided "as is" without express
  * or implied warranty.  By use of this software the user agrees to
  * indemnify and hold harmless RIOS Corporation from any  claims or
  * liability for loss arising out of such use.
  */

#include "ruby.h"

#ifdef HAVE_RUBY_REGEX_H
#  include "ruby/re.h"
#else
#  include "re.h"
#endif

// this sucks. but for some reason these moved around between 1.8 and 1.9
#ifdef ONIGURUMA_H
#define IGNORECASE ONIG_OPTION_IGNORECASE
#else
#define IGNORECASE RE_OPTION_IGNORECASE
#endif

#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <ibase.h>
#include <float.h>
#include <time.h>
#include <stdbool.h>


#define	SQLDA_COLSINIT	50
#define	SQLCODE_NOMORE	100
#define	TPBBUFF_ALLOC	64
#define	CMND_DELIMIT	" \t\n\r\f"
#define	LIST_DELIMIT	", \t\n\r\f"
#define	META_NAME_MAX	31

/* Statement type */
#define	STATEMENT_DDL	1
#define	STATEMENT_DML	0

/* Execute process flag */
#define	EXECF_EXECDML	0
#define	EXECF_SETPARM	1

static VALUE rb_mFb;
static VALUE rb_cFbDatabase;
static VALUE rb_cFbConnection;
static VALUE rb_cFbCursor;
static VALUE rb_cFbSqlType;
/* static VALUE rb_cFbGlobal; */
static VALUE rb_eFbError;
static VALUE rb_sFbField;
static VALUE rb_sFbIndex;
static VALUE rb_sFbColumn;
static VALUE rb_cDate;

static ID id_matches;
static ID id_downcase_bang;
static VALUE re_lowercase;
static ID id_rstrip_bang;
static ID id_sub_bang;
static ID id_force_encoding;

/* static char isc_info_stmt[] = { isc_info_sql_stmt_type }; */
/* static char isc_info_buff[16]; */
static char isc_tpb_0[] = {
    isc_tpb_version1,		isc_tpb_write,
    isc_tpb_concurrency,	isc_tpb_nowait
};

/* structs */

/* DB handle and TR parameter block list structure */
typedef struct
{
	isc_db_handle	*dbb_ptr ;
	long		 tpb_len ;
	char		*tpb_ptr ;
} ISC_TEB ; /* transaction existence block */

/* InterBase varchar structure */
typedef	struct
{
	short vary_length;
	char  vary_string[1];
} VARY;

struct FbConnection {
	isc_db_handle db;		/* DB handle */
	isc_tr_handle transact; /* transaction handle */
	VALUE cursor;
	unsigned short dialect;
	unsigned short db_dialect;
	short downcase_names;
	VALUE encoding;
	int dropped;
	ISC_STATUS isc_status[20];
	/* struct FbConnection *next; */
};

/* static struct FbConnection *fb_connection_list; */

struct FbCursor {
	int open;
	int eof;
	isc_tr_handle auto_transact;
	isc_stmt_handle stmt;
	XSQLDA *i_sqlda;
	XSQLDA *o_sqlda;
	char *i_buffer;
	long  i_buffer_size;
	char *o_buffer;
	long  o_buffer_size;
	VALUE fields_ary;
	VALUE fields_hash;
	VALUE connection;
};

typedef struct trans_opts
{
	const char *option1;
	const char *option2;
	char  optval;
	short position;
	struct trans_opts *sub_opts;
} trans_opts;

/* global utilities */

#define	FB_ALIGN(n, b)	((n + b - 1) & ~(b - 1))
#define	UPPER(c)	(((c) >= 'a' && (c)<= 'z') ? (c) - 'a' + 'A' : (c))
#define	FREE(p)		if (p)	{ xfree(p); p = 0; }
#define	SETNULL(p)	if (p && strlen(p) == 0)	{ p = 0; }
 // #define HERE(s) printf("%s\n", s)
#define HERE(s)

static long calculate_buffsize(XSQLDA *sqlda)
{
	XSQLVAR *var;
	long cols;
	short dtp;
	long offset = 0;
	long alignment;
	long length;
	long count;

	cols = sqlda->sqld;
	var = sqlda->sqlvar;
	for (count = 0; count < cols; var++,count++) {
		length = alignment = var->sqllen;
		dtp = var->sqltype & ~1;

		if (dtp == SQL_TEXT) {
			alignment = 1;
		} else if (dtp == SQL_VARYING) {
			length += sizeof(short);
			alignment = sizeof(short);
		}

		offset = FB_ALIGN(offset, alignment);
		offset += length;
		offset = FB_ALIGN(offset, sizeof(short));
		offset += sizeof(short);
	}

	return offset + sizeof(short);
}

#if (FB_API_VER >= 20)
static VALUE fb_error_msg(const ISC_STATUS *isc_status)
{
	char msg[1024];
	VALUE result = rb_str_new(NULL, 0);
	while (fb_interpret(msg, 1024, &isc_status))
	{
		result = rb_str_cat(result, msg, strlen(msg));
		result = rb_str_cat(result, "\n", strlen("\n"));
	}
	return result;
}
#else
static VALUE fb_error_msg(ISC_STATUS *isc_status)
{
	char msg[1024];
	VALUE result = rb_str_new(NULL, 0);
	while (isc_interprete(msg, &isc_status))
	{
		result = rb_str_cat(result, msg, strlen(msg));
		result = rb_str_cat(result, "\n", strlen("\n"));
	}
	return result;
}
#endif

struct time_object {
	struct timeval tv;
	struct tm tm;
	int gmt;
	int tm_got;
};

#define GetTimeval(obj, tobj) \
    Data_Get_Struct(obj, struct time_object, tobj)

static VALUE fb_mktime(struct tm *tm, const char *which)
{
#if defined(_WIN32)
	if (tm->tm_year + 1900 < 1970)
	{
		tm->tm_year = 70;
		tm->tm_mon = 0;
		tm->tm_mday = 1;
		tm->tm_hour = 0;
		tm->tm_min = 0;
		tm->tm_sec = 0;
	}
#endif
#if defined(_LP64) || defined(__LP64__) || defined(__arch64__)
// No need to floor time on 64-bit Unix.
#else
	if (tm->tm_year + 1900 < 1902)
	{
		tm->tm_year = 2;
		tm->tm_mon = 0;
		tm->tm_mday = 1;
		tm->tm_hour = 0;
		tm->tm_min = 0;
		tm->tm_sec = 0;
	}
#endif
	return rb_funcall(
		rb_cTime, rb_intern(which), 6,
		INT2FIX(tm->tm_year + 1900), INT2FIX(tm->tm_mon + 1), INT2FIX(tm->tm_mday),
		INT2FIX(tm->tm_hour), INT2FIX(tm->tm_min), INT2FIX(tm->tm_sec));
}

static VALUE fb_mkdate(struct tm *tm)
{
	return rb_funcall(
		rb_cDate, rb_intern("civil"), 3,
		INT2FIX(1900 + tm->tm_year), INT2FIX(tm->tm_mon + 1), INT2FIX(tm->tm_mday));
}

static int responds_like_date(VALUE obj)
{
	return rb_respond_to(obj, rb_intern("year")) &&
		rb_respond_to(obj, rb_intern("month")) &&
		rb_respond_to(obj, rb_intern("day"));
}
static void tm_from_date(struct tm *tm, VALUE date)
{
	VALUE year, month, day;

	if (!responds_like_date(date)) {
		VALUE s = rb_funcall(date, rb_intern("to_s"), 0);
		date = rb_funcall(rb_cDate, rb_intern("parse"), 1, s);
	}
	year = rb_funcall(date, rb_intern("year"), 0);
	month = rb_funcall(date, rb_intern("month"), 0);
	day = rb_funcall(date, rb_intern("day"), 0);
	memset(tm, 0, sizeof(struct tm));
	tm->tm_year = FIX2INT(year) - 1900;
	tm->tm_mon = FIX2INT(month) - 1;
	tm->tm_mday = FIX2INT(day);
}

static void tm_from_timestamp(struct tm *tm, VALUE obj)
{
#ifdef TypedData_Get_Struct
	VALUE year, month, day, hour, min, sec;
#else
	struct time_object *tobj;
#endif

	if (!rb_obj_is_kind_of(obj, rb_cTime))
	{
		if (rb_respond_to(obj, rb_intern("to_str")))
		{
			VALUE s = rb_funcall(obj, rb_intern("to_str"), 0);
			obj = rb_funcall(rb_cTime, rb_intern("parse"), 1, s);
		}
		else
		{
			rb_raise(rb_eTypeError, "Expecting Time object or string.");
		}
	}

#ifdef TypedData_Get_Struct
	year = rb_funcall(obj, rb_intern("year"), 0);
	month = rb_funcall(obj, rb_intern("month"), 0);
	day = rb_funcall(obj, rb_intern("day"), 0);
	hour = rb_funcall(obj, rb_intern("hour"), 0);
	min = rb_funcall(obj, rb_intern("min"), 0);
	sec = rb_funcall(obj, rb_intern("sec"), 0);
	memset(tm, 0, sizeof(struct tm));
	tm->tm_year = FIX2INT(year) - 1900;
	tm->tm_mon = FIX2INT(month) - 1;
	tm->tm_mday = FIX2INT(day);
	tm->tm_hour = FIX2INT(hour);
	tm->tm_min = FIX2INT(min);
	tm->tm_sec = FIX2INT(sec);
#else
	GetTimeval(obj, tobj);
	*tm = tobj->tm;
#endif
}

static VALUE object_to_fixnum(VALUE object)
{
	if (TYPE(object) != T_FIXNUM && TYPE(object) != T_BIGNUM)
	{
		if (TYPE(object) == T_FLOAT || !strcmp(rb_class2name(CLASS_OF(object)), "BigDecimal"))
			object = rb_funcall(object, rb_intern("round"), 0);
		else if (TYPE(object) == T_STRING)
			object = rb_funcall(rb_funcall(rb_mKernel, rb_intern("BigDecimal"), 1, object), rb_intern("round"), 0);
		else if (!strcmp(rb_class2name(CLASS_OF(object)), "Time"))
			rb_raise(rb_eTypeError, "Time value not allowed as Integer");
		else if (rb_respond_to(object, rb_intern("to_i")))
			object = rb_funcall(object, rb_intern("to_i"), 0);
		else
			rb_raise(rb_eTypeError, "Value doesn't respond to 'to_i' for conversion");
	}
	return (object);
}

static VALUE double_from_obj(VALUE obj)
{
	if (TYPE(obj) == T_STRING)
	{
		obj = rb_funcall(obj, rb_intern("to_f"), 0);
	}
	return obj;
}

static VALUE fb_sql_type_from_code(int code, int subtype)
{
	const char *sql_type = NULL;
	switch(code) {
		case SQL_TEXT:
		case blr_text:
			sql_type = "CHAR";
			break;
		case SQL_VARYING:
		case blr_varying:
			sql_type = "VARCHAR";
			break;
		case SQL_SHORT:
		case blr_short:
			switch (subtype) {
				case 0:		sql_type = "SMALLINT";	break;
				case 1:		sql_type = "NUMERIC";	break;
				case 2:		sql_type = "DECIMAL";	break;
			}
			break;
		case SQL_LONG:
		case blr_long:
			switch (subtype) {
				case 0:		sql_type = "INTEGER";	break;
				case 1:		sql_type = "NUMERIC";	break;
				case 2:		sql_type = "DECIMAL";	break;
			}
			break;
		case SQL_FLOAT:
		case blr_float:
			sql_type = "FLOAT";
			break;
		case SQL_DOUBLE:
		case blr_double:
			switch (subtype) {
				case 0:		sql_type = "DOUBLE PRECISION"; break;
				case 1:		sql_type = "NUMERIC";	break;
				case 2:		sql_type = "DECIMAL";	break;
			}
			break;
		case SQL_D_FLOAT:
		case blr_d_float:
			sql_type = "DOUBLE PRECISION";
			break;
		case SQL_TIMESTAMP:
		case blr_timestamp:
			sql_type = "TIMESTAMP";
			break;
		case SQL_BLOB:
		case blr_blob:
			sql_type = "BLOB";
			break;
		case SQL_ARRAY:
			sql_type = "ARRAY";
			break;
#if (FB_API_VER >= 30)
		case SQL_BOOLEAN:
		case blr_boolean:
			sql_type = "BOOLEAN";
			break;
#endif
		case SQL_QUAD:
		case blr_quad:
			sql_type = "DECIMAL";
			break;
		case SQL_TYPE_TIME:
		case blr_sql_time:
			sql_type = "TIME";
			break;
		case SQL_TYPE_DATE:
		case blr_sql_date:
			sql_type = "DATE";
			break;
		case SQL_INT64:
		case blr_int64:
			switch (subtype) {
				case 0:		sql_type = "BIGINT";	break;
				case 1:		sql_type = "NUMERIC";	break;
				case 2:		sql_type = "DECIMAL";	break;
			}
			break;
		default:
			printf("Unknown: %d, %d\n", code, subtype);
			sql_type = "UNKNOWN";
			break;
	}
	return rb_str_new2(sql_type);
}

/* call-seq:
 *   from_code(code, subtype) -> String
 *
 * Returns the SQL type, such as VARCHAR or INTEGER for a given type code and subtype.
 */
static VALUE sql_type_from_code(VALUE self, VALUE code, VALUE subtype)
{
	return fb_sql_type_from_code(NUM2INT(code), NUM2INT(subtype));
}

static void fb_error_check(ISC_STATUS *isc_status)
{
	if (isc_status[0] == 1 && isc_status[1]) {
		char buf[1024];
		VALUE exc, msg, msg1, msg2;
		short code = isc_sqlcode(isc_status);

		isc_sql_interprete(code, buf, 1024);
		msg1 = rb_str_new2(buf);
		msg2 = fb_error_msg(isc_status);
		msg = rb_str_cat(msg1, "\n", strlen("\n"));
		msg = rb_str_concat(msg, msg2);

		exc = rb_exc_new3(rb_eFbError, msg);
		rb_iv_set(exc, "error_code", INT2FIX(code));
		rb_exc_raise(exc);
	}
}

static void fb_error_check_warn(ISC_STATUS *isc_status)
{
	short code = isc_sqlcode(isc_status);
	if (code != 0) {
		char buf[1024];
		isc_sql_interprete(code, buf, 1024);
		rb_warning("%s(%d)", buf, code);
	}
}

static XSQLDA* sqlda_alloc(long cols)
{
	XSQLDA *sqlda;

	sqlda = (XSQLDA*)xmalloc(XSQLDA_LENGTH(cols));
#ifdef SQLDA_CURRENT_VERSION
	sqlda->version = SQLDA_CURRENT_VERSION;
#else
	sqlda->version = SQLDA_VERSION1;
#endif
	sqlda->sqln = cols;
	sqlda->sqld = 0;
	return sqlda;
}

static VALUE cursor_close _((VALUE));
static VALUE cursor_drop _((VALUE));
static VALUE cursor_execute _((int, VALUE*, VALUE));
static VALUE cursor_fetchall _((int, VALUE*, VALUE));

static void fb_cursor_mark();
static void fb_cursor_free();

/* connection utilities */
static void fb_connection_check(struct FbConnection *fb_connection)
{
	if (fb_connection->db == 0) {
		rb_raise(rb_eFbError, "closed db connection");
	}
}

static void fb_connection_close_cursors(struct FbConnection *fb_connection)
{
	int i;
	long len = RARRAY_LEN(fb_connection->cursor);
	for (i = 0; i < len; i++) {
		cursor_close(RARRAY_PTR(fb_connection->cursor)[i]);
	}
}

static void fb_connection_drop_cursors(struct FbConnection *fb_connection)
{
	int i;
	long len = RARRAY_LEN(fb_connection->cursor);
	for (i = 0; i < len; i++) {
		cursor_drop(RARRAY_PTR(fb_connection->cursor)[i]);
	}
  rb_ary_clear(fb_connection->cursor);
}

static void fb_connection_disconnect(struct FbConnection *fb_connection)
{
	if (fb_connection->transact) {
		isc_commit_transaction(fb_connection->isc_status, &fb_connection->transact);
		fb_error_check(fb_connection->isc_status);
	}
	if (fb_connection->dropped) {
		isc_drop_database(fb_connection->isc_status, &fb_connection->db);
	} else {
		isc_detach_database(fb_connection->isc_status, &fb_connection->db);
	}
	fb_error_check(fb_connection->isc_status);
}

static void fb_connection_disconnect_warn(struct FbConnection *fb_connection)
{
	if (fb_connection->transact) {
		isc_commit_transaction(fb_connection->isc_status, &fb_connection->transact);
		fb_error_check_warn(fb_connection->isc_status);
	}
	isc_detach_database(fb_connection->isc_status, &fb_connection->db);
	fb_error_check_warn(fb_connection->isc_status);
}

static void fb_connection_mark(struct FbConnection *fb_connection)
{
	rb_gc_mark(fb_connection->cursor);
}

static void fb_connection_free(struct FbConnection *fb_connection)
{
	if (fb_connection->db) {
		fb_connection_disconnect_warn(fb_connection);
	}
	xfree(fb_connection);
}

/*
static struct FbConnection* fb_connection_check_retrieve(VALUE data)
{
	if (TYPE(data) != T_DATA || RDATA(data)->dfree != (void *)fb_connection_free) {
		rb_raise(rb_eTypeError,
			"Wrong argument type %s (expected Fb::Connection)",
			rb_class2name(CLASS_OF(data)));
	}
	return (struct FbConnection*)RDATA(data)->data;
}
*/

static unsigned short fb_connection_db_SQL_Dialect(struct FbConnection *fb_connection)
{
	long dialect;
	long length;
	char db_info_command = isc_info_db_sql_dialect;
	char isc_info_buff[16];

	/* Get the db SQL Dialect */
	isc_database_info(fb_connection->isc_status, &fb_connection->db,
			1, &db_info_command,
			sizeof(isc_info_buff), isc_info_buff);
	fb_error_check(fb_connection->isc_status);

	if (isc_info_buff[0] == isc_info_db_sql_dialect) {
		length = isc_vax_integer(&isc_info_buff[1], 2);
		dialect = isc_vax_integer(&isc_info_buff[3], (short)length);
	} else {
		dialect = 1;
	}
	return dialect;
}

static unsigned short fb_connection_dialect(struct FbConnection *fb_connection)
{
	return fb_connection->dialect;
}

/*
static unsigned short fb_connection_db_dialect(struct FbConnection *fb_connection)
{
	return fb_connection->db_dialect;
}
*/

/* Transaction option list */

static trans_opts	rcom_opt_S[] =
{
	{"NO",			"RECORD_VERSION",	isc_tpb_no_rec_version,	-1,	0},
	{"RECORD_VERSION",	0,			isc_tpb_rec_version,	-1,	0},
	{"*",			0,			isc_tpb_no_rec_version,	-1,	0},
	{0,			0,			0,			0,	0}
};


static trans_opts	read_opt_S[] =
{
	{"WRITE",	0,	isc_tpb_write,		1,	0},
	{"ONLY",		0,	isc_tpb_read,		1,	0},
	{"COMMITTED",	0,	isc_tpb_read_committed,	2,	rcom_opt_S},
	{0,		0,	0,			0,	0}
};


static trans_opts	snap_opt_S[] =
{
	{"TABLE",	"STABILITY",	isc_tpb_consistency,	2,	0},
	{"*",		0,		isc_tpb_concurrency,	2,	0},
	{0,			0,	0,			0,	0}
};


static trans_opts	isol_opt_S[] =
{
	{"SNAPSHOT",	0,		0,			0,	snap_opt_S},
	{"READ",		"COMMITTED",	isc_tpb_read_committed,	2,	rcom_opt_S},
	{0,		0,		0,			0,	0}
};


static trans_opts	trans_opt_S[] =
{
	{"READ",		0,		0,		0,	read_opt_S},
	{"WAIT",		0,		isc_tpb_wait,	3,	0},
	{"NO",		"WAIT",		isc_tpb_nowait,	3,	0},
	{"ISOLATION",	"LEVEL",	0,		0,	isol_opt_S},
	{"SNAPSHOT",	0,		0,		0,	snap_opt_S},
	{"RESERVING",	0,		-1,		0,	0},
	{0,		0,		0,		0,	0}
};

/* Name1	Name2		Option value	    Position	Sub-option */

#define	RESV_TABLEEND	"FOR"
#define	RESV_SHARED	"SHARED"
#define	RESV_PROTECTD	"PROTECTED"
#define	RESV_READ	"READ"
#define	RESV_WRITE	"WRITE"
#define	RESV_CONTINUE	','

static char* trans_parseopts(VALUE opt, long *tpb_len)
{
	char *s, *trans;
	long used;
	long size;
	char *tpb;
	trans_opts *curr_p;
	trans_opts *target_p;
	char *check1_p;
	char *check2_p;
	int count;
	int next_c;
	char check_f[4];
	char *resv_p;
	char *resend_p;
	char *tblend_p = 0;
	long tbl_len;
	long res_first;
	int res_count;
	long ofs;
	char sp_prm;
	char rw_prm;
	int cont_f;
	const char *desc = 0;

	/* Initialize */
	s = StringValuePtr(opt);
	trans = ALLOCA_N(char, strlen(s)+1);
	strcpy(trans, s);
	s = trans;
	while (*s) {
		*s = UPPER(*s);
		s++;
	}

	used = 0;
	size = 0;
	tpb = NULL;
	memset((void *)check_f, 0, sizeof(check_f));

	/* Set the default transaction option */
	tpb = (char*)xmalloc(TPBBUFF_ALLOC);
	size = TPBBUFF_ALLOC;
	memcpy((void*)tpb, (void*)isc_tpb_0, sizeof(isc_tpb_0));
	used = sizeof(isc_tpb_0);

	/* Analize the transaction option strings */
	curr_p = trans_opt_S;
	check1_p = strtok(trans, CMND_DELIMIT);
	if (check1_p) {
		check2_p = strtok(0, CMND_DELIMIT);
	} else {
		check2_p = 0;
	}
	while (curr_p) {
		target_p = 0;
		next_c = 0;
		for (count = 0; curr_p[count].option1; count++) {
			if (!strcmp(curr_p[count].option1, "*")) {
				target_p = &curr_p[count];
				break;
			} else if (check1_p && !strcmp(check1_p, curr_p[count].option1)) {
				if (!curr_p[count].option2) {
					next_c = 1;
					target_p = &curr_p[count];
					break;
				} else if (check2_p && !strcmp(check2_p, curr_p[count].option2)) {
					next_c = 2;
					target_p = &curr_p[count];
					break;
				}
			}
		}

		if (!target_p) {
			desc = "Illegal transaction option was specified";
			goto error;
		}

		/* Set the transaction option */
		if (target_p->optval > '\0') {
			if (target_p->position > 0) {
				if (check_f[target_p->position]) {
					desc = "Duplicate transaction option was specified";
					goto error;
				}
				tpb[target_p->position] = target_p->optval;
				check_f[target_p->position] = 1;
			} else {
				if (used + 1 > size) {
					tpb = (char *)realloc(tpb, size + TPBBUFF_ALLOC);
					size += TPBBUFF_ALLOC;
				}
				tpb[used] = target_p->optval;
				used++;
			}
		} else if (target_p->optval) {		/* RESERVING ... FOR */
			if (check_f[0]) {
				desc = "Duplicate transaction option was specified";
				goto error;
			}
			resv_p = check2_p;
			if (!resv_p || !strcmp(resv_p, RESV_TABLEEND)) {
				desc = "RESERVING needs table name list";
				goto error;
			}
			while (resv_p) {
				res_first = used;
				res_count = 0;
				resend_p = strtok(0, CMND_DELIMIT);
				while (resend_p) {
					if (!strcmp(resend_p, RESV_TABLEEND)) {
						break;
					}
					resend_p = strtok(0, CMND_DELIMIT);
				}

				if (!resend_p) {
					desc = "Illegal transaction option was specified";
					goto error;
				}

				while (resv_p < resend_p) {
					if (*resv_p == '\0' || (ofs = strspn(resv_p, LIST_DELIMIT)) < 0) {
						resv_p++;
					} else {
						resv_p = &resv_p[ofs];
						tblend_p = strpbrk(resv_p, LIST_DELIMIT);
						if (tblend_p) {
							tbl_len = tblend_p - resv_p;
						} else {
							tbl_len = strlen(resv_p);
						}
						if (tbl_len > META_NAME_MAX) {
							desc = "Illegal table name was specified";
							goto error;
						}

						if (tbl_len > 0) {
							if (used + tbl_len + 3 > size) {
								tpb = (char*)xrealloc(tpb, size+TPBBUFF_ALLOC);
								size += TPBBUFF_ALLOC;
							}
							tpb[used+1] = (char)tbl_len;
							memcpy((void *)&tpb[used+2],resv_p, tbl_len);
							used += tbl_len + 3;
							res_count++;
						}
						resv_p += tbl_len;
					}
				}

				resv_p = strtok(0, CMND_DELIMIT);
				if (resv_p && !strcmp(resv_p, RESV_SHARED)) {
					sp_prm = isc_tpb_shared;
				} else if (resv_p && !strcmp(resv_p, RESV_PROTECTD)) {
					sp_prm = isc_tpb_protected;
				} else {
					desc = "RESERVING needs {SHARED|PROTECTED} {READ|WRITE}";
					goto error;
				}

				cont_f = 0;
				resv_p = strtok(0, CMND_DELIMIT);
				if (resv_p) {
					if (resv_p[strlen(resv_p)-1] == RESV_CONTINUE) {
						cont_f = 1;
						resv_p[strlen(resv_p)-1] = '\0';
					} else {
						tblend_p = strpbrk(resv_p, LIST_DELIMIT);
						if (tblend_p) {
							cont_f = 2;
							*tblend_p = '\0';
						}
					}
				}

				if (resv_p && !strcmp(resv_p, RESV_READ)) {
					rw_prm = isc_tpb_lock_read;
				} else if (resv_p && !strcmp(resv_p, RESV_WRITE)) {
					rw_prm = isc_tpb_lock_write;
				} else {
					desc = "RESERVING needs {SHARED|PROTECTED} {READ|WRITE}";
					goto error;
				}

				ofs = res_first;
				for (count = 0; count < res_count; count++) {
					tpb[ofs++] = rw_prm;
					ofs += tpb[ofs] + 1;
					tpb[ofs++] = sp_prm;
				}

				if (cont_f == 1) {
					resv_p = strtok(0, CMND_DELIMIT);
					if (!resv_p) {
						desc = "Unexpected end of command";
						goto error;
					}
				}
				if (cont_f == 2) {
					resv_p = tblend_p + 1;
				} else {
					resv_p = strtok(0, CMND_DELIMIT);
					if (resv_p) {
						if ((int)strlen(resv_p) == 1 && resv_p[0] == RESV_CONTINUE) {
							resv_p = strtok(0, CMND_DELIMIT);
							if (!resv_p) {
								desc = "Unexpected end of command";
								goto error;
							}
						} else if (resv_p[0] == RESV_CONTINUE) {
							resv_p++;
						} else {
							next_c = 1;
							check2_p = resv_p;
							resv_p = 0;
						}
					} else {
						next_c = 0;
						check1_p = check2_p = 0;
					}
				}
			}

			check_f[0] = 1;
		}


		/* Set the next check list */
		curr_p = target_p->sub_opts;

		for (count = 0; count < next_c; count++) {
			check1_p = check2_p;
			if (check2_p) {
				check2_p = strtok(0, CMND_DELIMIT);
			}
		}

		if (check1_p && !curr_p) {
			curr_p = trans_opt_S;
		}
	}

	/* Set the results */
	*tpb_len = used;
	return tpb;

error:
	xfree(tpb);
	rb_raise(rb_eFbError, "%s", desc);
}

static void fb_connection_transaction_start(struct FbConnection *fb_connection, VALUE opt)
{
	char *tpb = 0;
	long tpb_len;

	if (fb_connection->transact) {
		rb_raise(rb_eFbError, "A transaction has been already started");
	}

	if (!NIL_P(opt)) {
		tpb = trans_parseopts(opt, &tpb_len);
	} else {
		tpb_len = 0;
		tpb = NULL;
	}

	isc_start_transaction(fb_connection->isc_status, &fb_connection->transact, 1, &fb_connection->db, tpb_len, tpb);
	xfree(tpb);
	fb_error_check(fb_connection->isc_status);
}

static void fb_connection_commit(struct FbConnection *fb_connection)
{
	if (fb_connection->transact) {
		fb_connection_close_cursors(fb_connection);
		isc_commit_transaction(fb_connection->isc_status, &fb_connection->transact);
		fb_error_check(fb_connection->isc_status);
	}
}

static void fb_connection_rollback(struct FbConnection *fb_connection)
{
	if (fb_connection->transact) {
		fb_connection_close_cursors(fb_connection);
		isc_rollback_transaction(fb_connection->isc_status, &fb_connection->transact);
		fb_error_check(fb_connection->isc_status);
	}
}

/* call-seq:
 *   transaction(options) -> true
 *   transaction(options) { } -> block result
 *
 * Start a transaction for this connection.
 */
static VALUE connection_transaction(int argc, VALUE *argv, VALUE self)
{
	struct FbConnection *fb_connection;
	VALUE opt = Qnil;

	rb_scan_args(argc, argv, "01", &opt);
	Data_Get_Struct(self, struct FbConnection, fb_connection);

	fb_connection_transaction_start(fb_connection, opt);

	if (rb_block_given_p()) {
		int state;
		VALUE result = rb_protect(rb_yield, 0, &state);
		if (state) {
			fb_connection_rollback(fb_connection);
			return rb_funcall(rb_mKernel, rb_intern("raise"), 0);
		} else {
			fb_connection_commit(fb_connection);
			return result;
		}
	} else {
		return Qtrue;
   	}
}

/* call-seq:
 *   transaction_started()? -> true or false
 *
 * Returns true if a transaction is currently active.
 */
static VALUE connection_transaction_started(VALUE self)
{
	struct FbConnection *fb_connection;
	Data_Get_Struct(self, struct FbConnection, fb_connection);

	return fb_connection->transact ? Qtrue : Qfalse;
}

/* call-seq:
 *   commit() -> nil
 *
 * Commit the current transaction.
 */
static VALUE connection_commit(VALUE self)
{
	struct FbConnection *fb_connection;
	Data_Get_Struct(self, struct FbConnection, fb_connection);

	fb_connection_commit(fb_connection);
	return Qnil;
}

/* call-seq:
 *   rollback() -> nil
 *
 * Rollback the current transaction.
 */
static VALUE connection_rollback(VALUE self)
{
	struct FbConnection *fb_connection;
	Data_Get_Struct(self, struct FbConnection, fb_connection);

	fb_connection_rollback(fb_connection);
	return Qnil;
}

/*
 * call-seq:
 *   open?() -> true or false
 *
 * Current connection status.
 */

static VALUE connection_is_open(VALUE self)
{
	struct FbConnection *fb_connection;

	Data_Get_Struct(self, struct FbConnection, fb_connection);
	return (fb_connection->db == 0) ? Qfalse : Qtrue;
}

/* call-seq:
 *   to_s() -> String
 *
 * Return current database connection string and either (OPEN) or (CLOSED).
 */
static VALUE connection_to_s(VALUE self)
{
	VALUE is_open = connection_is_open(self);
	VALUE status = (is_open == Qtrue) ? rb_str_new2(" (OPEN)") : rb_str_new2(" (CLOSED)");
	VALUE database = rb_iv_get(self, "@database");
	VALUE s = rb_str_dup(database);
	return rb_str_concat(s, status);
}

/* call-seq:
 *   cursor() -> Cursor
 *
 * Creates a +Cursor+ for the +Connection+ and allocates a statement.
 * This function is no longer published.
 */
static VALUE connection_cursor(VALUE self)
{
	VALUE c;
	struct FbConnection *fb_connection;
	struct FbCursor *fb_cursor;

	Data_Get_Struct(self, struct FbConnection, fb_connection);
	fb_connection_check(fb_connection);

	c = Data_Make_Struct(rb_cFbCursor, struct FbCursor, fb_cursor_mark, fb_cursor_free, fb_cursor);
	fb_cursor->connection = self;
	fb_cursor->fields_ary = Qnil;
	fb_cursor->fields_hash = Qnil;
	fb_cursor->open = Qfalse;
	fb_cursor->eof = Qfalse;
	fb_cursor->stmt = 0;
	fb_cursor->i_sqlda = sqlda_alloc(SQLDA_COLSINIT);
	fb_cursor->o_sqlda = sqlda_alloc(SQLDA_COLSINIT);
	fb_cursor->i_buffer = NULL;
	fb_cursor->i_buffer_size = 0;
	fb_cursor->o_buffer = NULL;
	fb_cursor->o_buffer_size = 0;
	isc_dsql_alloc_statement2(fb_connection->isc_status, &fb_connection->db, &fb_cursor->stmt);
	fb_error_check(fb_connection->isc_status);

	return c;
}

/* call-seq:
 *   execute(sql, *args) -> Cursor or rows affected
 *   execute(sql, *args) {|cursor| } -> block result
 *
 * Allocates a +Cursor+ and executes the +sql+ statement, matching up the
 * parameters in +args+ with the place holders in the statement, represented by ?'s.
 *
 * If the sql statement returns a result set and a block is provided, the cursor is
 * yielded to the block before being automatically closed.
 *
 * If the sql statement returns a result set and a block is not provided, a +Cursor+
 * object is returned.
 *
 * If the sql statement performs an INSERT, UPDATE or DELETE, the number of rows
 * affected is returned.  Other statements, such as schema updates, return -1.
 *
 * +*args+ can be one or more tuples of parameters or an array of tuples (arrays),
 * in which case the statement will be executed once for each tuple of parameters.
 *
 * If no transaction is currently active, a transaction is automatically started
 * and is closed when the cursor is closed.  Note that if additional statements
 * yielding cursors are started before the first cursor is closed, these cursors will
 * also be closed when the first one is closed and its transaction committed.
 */
static VALUE connection_execute(int argc, VALUE *argv, VALUE self)
{
	VALUE cursor = connection_cursor(self);
	VALUE val = cursor_execute(argc, argv, cursor);

	if (NIL_P(val)) {
		if (rb_block_given_p()) {
			return rb_ensure(rb_yield,cursor,cursor_close,cursor);
   		} else {
				HERE("connection_execute Y");
				return cursor;
   		}
	} else {
		cursor_drop(cursor);
	}
	return val;
}

/* call-seq:
 *   query(:array, sql, *arg) -> Array of Arrays or nil
 *   query(:hash, sql, *arg) -> Array of Hashes or nil
 *   query(sql, *args) -> Array of Arrays or nil
 *
 * For queries returning a result set, an array is returned, containing
 * either a list of Arrays or Hashes, one for each row.
 *
 * If the sql statement performs an INSERT, UPDATE or DELETE, the number of rows
 * affected is returned.  Other statements, such as schema updates, return -1.
 *
 * If no transaction is currently active, a transaction is automatically started
 * and committed.  Otherwise, the statement executes within the context of the
 * current transaction.
 */
static VALUE connection_query(int argc, VALUE *argv, VALUE self)
{
	VALUE format;
	VALUE cursor;
	VALUE result;

	if (argc >= 1 && TYPE(argv[0]) == T_SYMBOL) {
		format = argv[0];
		argc--; argv++;
	} else {
		format = ID2SYM(rb_intern("array"));
	}
	cursor = connection_cursor(self);
	result = cursor_execute(argc, argv, cursor);
	if (NIL_P(result)) {
		result = cursor_fetchall(1, &format, cursor);
		cursor_close(cursor);
	}

	return result;
}

/* call-seq:
 *   close() -> nil
 *
 * Closes connection to database.  All open cursors are dropped.
 */
static VALUE connection_close(VALUE self)
{
	struct FbConnection *fb_connection;

	Data_Get_Struct(self, struct FbConnection, fb_connection);

	if (fb_connection->dropped) return Qnil;

	fb_connection_check(fb_connection);
	fb_connection_disconnect(fb_connection);
	fb_connection_drop_cursors(fb_connection);

	return Qnil;
}

/* call-seq:
 *   drop() -> nil
 *
 * Drops connected database.  All open cursors are dropped.
 */
static VALUE connection_drop(VALUE self)
{
	struct FbConnection *fb_connection;

	Data_Get_Struct(self, struct FbConnection, fb_connection);
	fb_connection->dropped = 1;
	fb_connection_disconnect(fb_connection);
	fb_connection_drop_cursors(fb_connection);

	return Qnil;
}

/* call-seq:
 *   dialect() -> int
 *
 * Returns dialect of connection.
 */
static VALUE connection_dialect(VALUE self)
{
	struct FbConnection *fb_connection;

	Data_Get_Struct(self, struct FbConnection, fb_connection);
	fb_connection_check(fb_connection);

	return INT2FIX(fb_connection->dialect);
}

/* call-seq:
 *   db_dialect() -> int
 *
 * Returns database dialect.
 */
static VALUE connection_db_dialect(VALUE self)
{
	struct FbConnection *fb_connection;

	Data_Get_Struct(self, struct FbConnection, fb_connection);
	fb_connection_check(fb_connection);

	return INT2FIX(fb_connection->db_dialect);
}

static void fb_cursor_check(struct FbCursor *fb_cursor)
{
	if (fb_cursor->stmt == 0) {
		rb_raise(rb_eFbError, "dropped db cursor");
	}
	if (!fb_cursor->open) {
		rb_raise(rb_eFbError, "closed db cursor");
	}
}

static void fb_cursor_drop(struct FbCursor *fb_cursor)
{
	ISC_STATUS isc_status[20];
	if (fb_cursor->open) {
		isc_dsql_free_statement(isc_status, &fb_cursor->stmt, DSQL_close);
		fb_error_check(isc_status);
	}
	isc_dsql_free_statement(isc_status, &fb_cursor->stmt, DSQL_drop);
	fb_error_check(isc_status);
}

static void fb_cursor_drop_warn(struct FbCursor *fb_cursor)
{
	ISC_STATUS isc_status[20];
	if (fb_cursor->open) {
		isc_dsql_free_statement(isc_status, &fb_cursor->stmt, DSQL_close);
		fb_error_check_warn(isc_status);
	}
	isc_dsql_free_statement(isc_status, &fb_cursor->stmt, DSQL_drop);
	fb_error_check_warn(isc_status);
}

static void fb_cursor_mark(struct FbCursor *fb_cursor)
{
	rb_gc_mark(fb_cursor->connection);
	rb_gc_mark(fb_cursor->fields_ary);
	rb_gc_mark(fb_cursor->fields_hash);
}

static void fb_cursor_free(struct FbCursor *fb_cursor)
{
	if (fb_cursor->stmt) {
		fb_cursor_drop_warn(fb_cursor);
	}
	xfree(fb_cursor->i_sqlda);
	xfree(fb_cursor->o_sqlda);
	xfree(fb_cursor->i_buffer);
	xfree(fb_cursor->o_buffer);
	xfree(fb_cursor);
}

static VALUE sql_decimal_to_bigdecimal(long long sql_data, int scale)
{
	unsigned long i;
	char bigdecimal_buffer[23];
	unsigned long bigdecimal_dot;
	sprintf(bigdecimal_buffer, "%022lld", sql_data);
	bigdecimal_dot = strlen(bigdecimal_buffer) + scale;
	for (i = strlen(bigdecimal_buffer); i > bigdecimal_dot; i--)
		bigdecimal_buffer[i] = bigdecimal_buffer[i-1];
	bigdecimal_buffer[bigdecimal_dot] = '.';
	return rb_funcall(rb_cObject, rb_intern("BigDecimal"), 1, rb_str_new2(bigdecimal_buffer));
}

static VALUE object_to_unscaled_bigdecimal(VALUE object, int scale)
{
	int i;
	long ratio = 1;
	for (i = 0; i > scale; i--)
		ratio *= 10;
	if (TYPE(object) == T_FLOAT)
		object = rb_funcall(object, rb_intern("to_s"), 0);
	object = rb_funcall(rb_cObject, rb_intern("BigDecimal"), 1, object);
	object = rb_funcall(object, rb_intern("*"), 1, LONG2NUM(ratio));
	return rb_funcall(object, rb_intern("round"), 0);
}

static void fb_cursor_set_inputparams(struct FbCursor *fb_cursor, long argc, VALUE *argv)
{
	struct FbConnection *fb_connection;
	long count;
	long offset;
	/* long type; */
	short dtp;
	VALUE obj;
	long lvalue;
	ISC_INT64 llvalue;
	long alignment;
	double dvalue;
	double dcheck;
	VARY *vary;
	XSQLVAR *var;

	isc_blob_handle blob_handle;
	ISC_QUAD blob_id;
	/* static char blob_items[] = { isc_info_blob_max_segment }; */
	/* char blob_info[16]; */
	char *p;
	long length;
	/* struct time_object *tobj; */
	struct tm tms;

	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);

	/* Check the number of parameters */
	if (fb_cursor->i_sqlda->sqld != argc) {
		rb_raise(rb_eFbError, "statement requires %d items; %ld given", fb_cursor->i_sqlda->sqld, argc);
	}

	/* Get the parameters */
	for (count = 0,offset = 0; count < argc; count++) {
		obj = argv[count];

		/* type = TYPE(obj); */

		/* Convert the data type for InterBase */
		var = &fb_cursor->i_sqlda->sqlvar[count];
		if (!NIL_P(obj)) {
			dtp = var->sqltype & ~1;		/* Erase null flag */
			alignment = var->sqllen;

			switch (dtp) {
				case SQL_TEXT :
					alignment = 1;
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					obj = rb_obj_as_string(obj);
					if (RSTRING_LEN(obj) > var->sqllen) {
						rb_raise(rb_eRangeError, "CHAR overflow: %ld bytes exceeds %d byte(s) allowed.",
							RSTRING_LEN(obj), var->sqllen);
					}
					memcpy(var->sqldata, RSTRING_PTR(obj), RSTRING_LEN(obj));
					var->sqllen = RSTRING_LEN(obj);
					offset += var->sqllen + 1;
					break;

				case SQL_VARYING :
					alignment = sizeof(short);
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					vary = (VARY *)var->sqldata;
					obj = rb_obj_as_string(obj);
					if (RSTRING_LEN(obj) > var->sqllen) {
						rb_raise(rb_eRangeError, "VARCHAR overflow: %ld bytes exceeds %d byte(s) allowed.",
							RSTRING_LEN(obj), var->sqllen);
					}
					memcpy(vary->vary_string, RSTRING_PTR(obj), RSTRING_LEN(obj));
					vary->vary_length = RSTRING_LEN(obj);
					offset += vary->vary_length + sizeof(short);
					break;

				case SQL_SHORT :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					if (var->sqlscale < 0) {
						lvalue = NUM2LONG(object_to_unscaled_bigdecimal(obj, var->sqlscale));
					} else {
						lvalue = NUM2LONG(object_to_fixnum(obj));
					}
					if (lvalue < -32768 || lvalue > 32767) {
						rb_raise(rb_eRangeError, "short integer overflow");
					}
					*(short *)var->sqldata = lvalue;
					offset += alignment;
					break;

				case SQL_LONG :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					if (var->sqlscale < 0) {
						lvalue = NUM2LONG(object_to_unscaled_bigdecimal(obj, var->sqlscale));
					} else {
						lvalue = NUM2LONG(object_to_fixnum(obj));
					}
					if (lvalue < -2147483648LL || lvalue > 2147483647LL) {
						rb_raise(rb_eRangeError, "integer overflow");
					}
					*(ISC_LONG *)var->sqldata = (ISC_LONG)lvalue;
					offset += alignment;
					break;

				case SQL_FLOAT :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					obj = double_from_obj(obj);
					dvalue = NUM2DBL(obj);
					if (dvalue >= 0.0) {
						dcheck = dvalue;
					} else {
						dcheck = dvalue * -1;
					}
					if (dcheck != 0.0 && (dcheck < FLT_MIN || dcheck > FLT_MAX)) {
						rb_raise(rb_eRangeError, "float overflow");
					}
					*(float *)var->sqldata = (float)dvalue;
					offset += alignment;
					break;

				case SQL_DOUBLE :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					obj = double_from_obj(obj);
					dvalue = NUM2DBL(obj);
					*(double *)var->sqldata = dvalue;
					offset += alignment;
					break;

				case SQL_INT64 :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);

					if (var->sqlscale < 0) {
						llvalue = NUM2LL(object_to_unscaled_bigdecimal(obj, var->sqlscale));
					} else {
						llvalue = NUM2LL(object_to_fixnum(obj));
					}

					*(ISC_INT64 *)var->sqldata = llvalue;
					offset += alignment;
					break;

				case SQL_BLOB :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					obj = rb_obj_as_string(obj);

					blob_handle = 0;
					isc_create_blob2(
						fb_connection->isc_status,&fb_connection->db,&fb_connection->transact,
						&blob_handle,&blob_id,0,NULL);
					fb_error_check(fb_connection->isc_status);
					length = RSTRING_LEN(obj);
					p = RSTRING_PTR(obj);
					while (length >= 4096) {
						isc_put_segment(fb_connection->isc_status,&blob_handle,4096,p);
						fb_error_check(fb_connection->isc_status);
						p += 4096;
						length -= 4096;
					}
					if (length) {
						isc_put_segment(fb_connection->isc_status,&blob_handle,length,p);
						fb_error_check(fb_connection->isc_status);
					}
					isc_close_blob(fb_connection->isc_status,&blob_handle);
					fb_error_check(fb_connection->isc_status);

					*(ISC_QUAD *)var->sqldata = blob_id;
					offset += alignment;
					break;

				case SQL_TIMESTAMP :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					tm_from_timestamp(&tms, obj);
					isc_encode_timestamp(&tms, (ISC_TIMESTAMP *)var->sqldata);
					offset += alignment;
					break;

				case SQL_TYPE_TIME :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					tm_from_timestamp(&tms, obj);
					isc_encode_sql_time(&tms, (ISC_TIME *)var->sqldata);
					offset += alignment;
					break;

				case SQL_TYPE_DATE :
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					tm_from_date(&tms, obj);
					isc_encode_sql_date(&tms, (ISC_DATE *)var->sqldata);
					offset += alignment;
					break;

#if 0
				case SQL_ARRAY :
					/* Not supported now
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					if (get_arrayvalue(self, type, obj, var))
						return(STATUS_ABNORMAL);
					offset += alignment;
					break;
					*/
					rb_raise(rb_eArgError, "Arrays not supported");
					break;
#endif

#if (FB_API_VER >= 30)
				case SQL_BOOLEAN:
					offset = FB_ALIGN(offset, alignment);
					var->sqldata = (char *)(fb_cursor->i_buffer + offset);
					*(bool *)var->sqldata = obj;
					offset += alignment;
					break;
#endif
				default :
					rb_raise(rb_eFbError, "Specified table includes unsupported datatype (%d)", dtp);
			}

			if (var->sqltype & 1) {
				offset = FB_ALIGN(offset, sizeof(short));
				var->sqlind = (short *)(fb_cursor->i_buffer + offset);
				*var->sqlind = 0;
				offset += sizeof(short);
			}
		} else if (var->sqltype & 1) {
			var->sqldata = 0;
			offset = FB_ALIGN(offset, sizeof(short));
			var->sqlind = (short *)(fb_cursor->i_buffer + offset);
			*var->sqlind = -1;
			offset += sizeof(short);
		} else {
			rb_raise(rb_eFbError, "specified column is not permitted to be null");
		}
	}
}

static void fb_cursor_execute_withparams(struct FbCursor *fb_cursor, long argc, VALUE *argv)
{
	struct FbConnection *fb_connection;

	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);
	/* Check the first object type of the parameters */
	if (argc >= 1 && TYPE(argv[0]) == T_ARRAY) {
		int i;
		VALUE obj;
		VALUE ary = argv[0];
		if (RARRAY_LEN(ary) > 0 && TYPE(RARRAY_PTR(ary)[0]) == T_ARRAY) {
			for (i = 0; i < RARRAY_LEN(ary); i++) {
				obj = rb_ary_entry(ary, i);
				fb_cursor_execute_withparams(fb_cursor, 1, &obj);
			}
		} else {
			for (i = 0; i < argc; i++) {
				obj = argv[i];

				/* Set the input parameters */
				Check_Type(obj, T_ARRAY);
				fb_cursor_set_inputparams(fb_cursor, RARRAY_LEN(obj), RARRAY_PTR(obj));

				/* Execute SQL statement */
				isc_dsql_execute2(fb_connection->isc_status, &fb_connection->transact, &fb_cursor->stmt, SQLDA_VERSION1, fb_cursor->i_sqlda, NULL);
				fb_error_check(fb_connection->isc_status);
			}
		}
	} else {
		/* Set the input parameters */
		fb_cursor_set_inputparams(fb_cursor, argc, argv);

		/* Execute SQL statement */
		isc_dsql_execute2(fb_connection->isc_status, &fb_connection->transact, &fb_cursor->stmt, SQLDA_VERSION1, fb_cursor->i_sqlda, NULL);
		fb_error_check(fb_connection->isc_status);
	}
}

static VALUE precision_from_sqlvar(XSQLVAR *sqlvar)
{
	switch(sqlvar->sqltype & ~1) {
		case SQL_TEXT:		return Qnil;
		case SQL_VARYING:	return Qnil;
		case SQL_SHORT:
			switch (sqlvar->sqlsubtype) {
				case 0:		return INT2FIX(0);
				case 1:		return INT2FIX(4);
				case 2:		return INT2FIX(4);
			}
			break;
		case SQL_LONG:
			switch (sqlvar->sqlsubtype) {
				case 0:		return INT2FIX(0);
				case 1:		return INT2FIX(9);
				case 2:		return INT2FIX(9);
			}
			break;
		case SQL_FLOAT:		return Qnil;
		case SQL_DOUBLE:
		case SQL_D_FLOAT:
			switch (sqlvar->sqlsubtype) {
				case 0:		return Qnil;
				case 1:		return INT2FIX(15);
				case 2:		return INT2FIX(15);
			}
			break;
		case SQL_TIMESTAMP:	return Qnil;
		case SQL_BLOB:		return Qnil;
		case SQL_ARRAY:		return Qnil;
#if (FB_API_VER >= 30)
		case SQL_BOOLEAN: return Qnil;
#endif
		case SQL_QUAD:		return Qnil;
		case SQL_TYPE_TIME:	return Qnil;
		case SQL_TYPE_DATE:	return Qnil;
		case SQL_INT64:
			switch (sqlvar->sqlsubtype) {
				case 0:		return INT2FIX(0);
				case 1:		return INT2FIX(18);
				case 2:		return INT2FIX(18);
			}
			break;
	}
	return Qnil;
}

static int no_lowercase(VALUE value)
{
	VALUE local_value = StringValue(value);
	int result = rb_funcall(re_lowercase, id_matches, 1, local_value) == Qnil;
	return result;
}

static VALUE fb_cursor_fields_ary(XSQLDA *sqlda, short downcase_names)
{
	long cols;
	long count;
	XSQLVAR *var;
	short dtp;
	VALUE ary;

	cols = sqlda->sqld;
	if (cols == 0) {
		return Qnil;
	}

	ary = rb_ary_new();
	for (count = 0; count < cols; count++) {
		VALUE field;
		VALUE name, type_code, sql_type, sql_subtype, display_size, internal_size, precision, scale, nullable;

		var = &sqlda->sqlvar[count];
		dtp = var->sqltype & ~1;

		if (var->aliasname_length) { /* aliasname always present? */
			name = rb_tainted_str_new(var->aliasname, var->aliasname_length);
		} else {
			name = rb_tainted_str_new(var->sqlname, var->sqlname_length);
		}
		if (downcase_names && no_lowercase(name)) {
			rb_funcall(name, id_downcase_bang, 0);
		}
		rb_str_freeze(name);
		type_code = INT2NUM((long)(var->sqltype & ~1));
		sql_type = fb_sql_type_from_code(dtp, var->sqlsubtype);
		sql_subtype = INT2FIX(var->sqlsubtype);
		display_size = INT2NUM((long)var->sqllen);
		if (dtp == SQL_VARYING) {
			internal_size = INT2NUM((long)var->sqllen + sizeof(short));
		} else {
			internal_size = INT2NUM((long)var->sqllen);
		}
		precision = precision_from_sqlvar(var);
		scale = INT2NUM((long)var->sqlscale);
		nullable = (var->sqltype & 1) ? Qtrue : Qfalse;

		field = rb_struct_new(rb_sFbField, name, sql_type, sql_subtype, display_size, internal_size, precision, scale, nullable, type_code);
		rb_ary_push(ary, field);
	}
	rb_ary_freeze(ary);
	return ary;
}

static VALUE fb_cursor_fields_hash(VALUE fields_ary)
{
	int i;
	VALUE hash = rb_hash_new();

	for (i = 0; i < RARRAY_LEN(fields_ary); i++) {
		VALUE field = rb_ary_entry(fields_ary, i);
		VALUE name = rb_struct_aref(field, LONG2NUM(0));
		rb_hash_aset(hash, name, field);
	}

	return hash;
}

static void fb_cursor_fetch_prep(struct FbCursor *fb_cursor)
{
	struct FbConnection *fb_connection;
	long cols;
	long count;
	XSQLVAR *var;
	short dtp;
	long length;
	long alignment;
	long offset;

	fb_cursor_check(fb_cursor);

	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);
	fb_connection_check(fb_connection);

	/* Check if open cursor */
	if (!fb_cursor->open) {
		rb_raise(rb_eFbError, "The cursor has not been opened. Use execute(query)");
	}
	/* Describe output SQLDA */
	isc_dsql_describe(fb_connection->isc_status, &fb_cursor->stmt, 1, fb_cursor->o_sqlda);
	fb_error_check(fb_connection->isc_status);

	/* Set the output SQLDA */
	cols = fb_cursor->o_sqlda->sqld;
	for (var = fb_cursor->o_sqlda->sqlvar, offset = 0, count = 0; count < cols; var++, count++) {
		length = alignment = var->sqllen;
		dtp = var->sqltype & ~1;

		if (dtp == SQL_TEXT) {
			alignment = 1;
		} else if (dtp == SQL_VARYING) {
			length += sizeof(short);
			alignment = sizeof(short);
		}
		offset = FB_ALIGN(offset, alignment);
		var->sqldata = (char*)(fb_cursor->o_buffer + offset);
		offset += length;
		offset = FB_ALIGN(offset, sizeof(short));
		var->sqlind = (short*)(fb_cursor->o_buffer + offset);
		offset += sizeof(short);
	}
}

static VALUE fb_cursor_fetch(struct FbCursor *fb_cursor)
{
	struct FbConnection *fb_connection;
	long cols;
	VALUE ary;
	long count;
	XSQLVAR *var;
	long dtp;
	VALUE val;
	VARY *vary;
	struct tm tms;

	isc_blob_handle blob_handle;
	ISC_QUAD blob_id;
	unsigned short actual_seg_len;
	static char blob_items[] = {
		isc_info_blob_max_segment,
		isc_info_blob_num_segments,
		isc_info_blob_total_length
	};
	char blob_info[32];
	char *p, item;
	short length;
	unsigned short max_segment = 0;
	ISC_LONG num_segments = 0;
	ISC_LONG total_length = 0;

	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);
	fb_connection_check(fb_connection);

	if (fb_cursor->eof) {
		rb_raise(rb_eFbError, "Cursor is past end of data.");
	}
	/* Fetch one row */
	if (isc_dsql_fetch(fb_connection->isc_status, &fb_cursor->stmt, 1, fb_cursor->o_sqlda) == SQLCODE_NOMORE) {
		fb_cursor->eof = Qtrue;
		return Qnil;
	}
	fb_error_check(fb_connection->isc_status);

	/* Create the result tuple object */
	cols = fb_cursor->o_sqlda->sqld;
	ary = rb_ary_new2(cols);

	/* Create the result objects for each columns */
	for (count = 0; count < cols; count++) {
		var = &fb_cursor->o_sqlda->sqlvar[count];
		dtp = var->sqltype & ~1;

		/* Check if column is null */
		if ((var->sqltype & 1) && (*var->sqlind < 0)) {
			val = Qnil;
		} else {
			/* Set the column value to the result tuple */

			switch (dtp) {
				case SQL_TEXT:
					val = rb_tainted_str_new(var->sqldata, var->sqllen);
					#if HAVE_RUBY_ENCODING_H
					rb_funcall(val, id_force_encoding, 1, fb_connection->encoding);
					#endif
					break;

				case SQL_VARYING:
					vary = (VARY*)var->sqldata;
					val = rb_tainted_str_new(vary->vary_string, vary->vary_length);
					#if HAVE_RUBY_ENCODING_H
					rb_funcall(val, id_force_encoding, 1, fb_connection->encoding);
					#endif
					break;

				case SQL_SHORT:
					if (var->sqlscale < 0) {
						val = sql_decimal_to_bigdecimal((long long)*(ISC_SHORT*)var->sqldata, var->sqlscale);
					} else {
						val = INT2NUM((long)*(short*)var->sqldata);
					}
					break;

				case SQL_LONG:
					if (var->sqlscale < 0) {
						val = sql_decimal_to_bigdecimal((long long)*(ISC_LONG*)var->sqldata, var->sqlscale);
					} else {
						val = INT2NUM(*(ISC_LONG*)var->sqldata);
					}
					break;

				case SQL_FLOAT:
					val = rb_float_new((double)*(float*)var->sqldata);
					break;

				case SQL_DOUBLE:
					val = rb_float_new(*(double*)var->sqldata);
					break;

				case SQL_INT64:
					if (var->sqlscale < 0) {
						val = sql_decimal_to_bigdecimal(*(ISC_INT64*)var->sqldata, var->sqlscale);
					} else {
						val = LL2NUM(*(ISC_INT64*)var->sqldata);
					}
					break;

				case SQL_TIMESTAMP:
					isc_decode_timestamp((ISC_TIMESTAMP *)var->sqldata, &tms);
					val = fb_mktime(&tms, "local");
					break;

				case SQL_TYPE_TIME:
					isc_decode_sql_time((ISC_TIME *)var->sqldata, &tms);
					tms.tm_year = 100;
					tms.tm_mon = 0;
					tms.tm_mday = 1;
					val = fb_mktime(&tms, "utc");
					break;

				case SQL_TYPE_DATE:
					isc_decode_sql_date((ISC_DATE *)var->sqldata, &tms);
					val = fb_mkdate(&tms);
					break;

				case SQL_BLOB:
					blob_handle = 0;
					blob_id = *(ISC_QUAD *)var->sqldata;
					isc_open_blob2(fb_connection->isc_status, &fb_connection->db, &fb_connection->transact, &blob_handle, &blob_id, 0, NULL);
					fb_error_check(fb_connection->isc_status);
					isc_blob_info(
						fb_connection->isc_status, &blob_handle,
						sizeof(blob_items), blob_items,
						sizeof(blob_info), blob_info);
					fb_error_check(fb_connection->isc_status);
					for (p = blob_info; *p != isc_info_end; p += length) {
						item = *p++;
						length = (short) isc_vax_integer(p,2);
						p += 2;
						switch (item) {
							case isc_info_blob_max_segment:
								max_segment = isc_vax_integer(p,length);
								break;
							case isc_info_blob_num_segments:
								num_segments = isc_vax_integer(p,length);
								break;
							case isc_info_blob_total_length:
								total_length = isc_vax_integer(p,length);
								break;
						}
					}
					val = rb_tainted_str_new(NULL,total_length);
					for (p = RSTRING_PTR(val); num_segments > 0; num_segments--, p += actual_seg_len) {
						isc_get_segment(fb_connection->isc_status, &blob_handle, &actual_seg_len, max_segment, p);
						fb_error_check(fb_connection->isc_status);
					}
					#if HAVE_RUBY_ENCODING_H
					rb_funcall(val, id_force_encoding, 1, fb_connection->encoding);
					#endif
					isc_close_blob(fb_connection->isc_status, &blob_handle);
					fb_error_check(fb_connection->isc_status);
					break;

				case SQL_ARRAY:
					rb_warn("ARRAY not supported (yet)");
					val = Qnil;
					break;

#if (FB_API_VER >= 30)
				case SQL_BOOLEAN:
					val = (*(bool*)var->sqldata) ? Qtrue : Qfalse;
					break;
#endif

				default:
					rb_raise(rb_eFbError, "Specified table includes unsupported datatype (%ld)", dtp);
					break;
			}
		}
		rb_ary_push(ary, val);
	}

	return ary;
}

static long cursor_rows_affected(struct FbCursor *fb_cursor, long statement_type)
{
	long inserted = 0, selected = 0, updated = 0, deleted = 0;
	char request[] = { isc_info_sql_records };
	char response[64], *r;
	ISC_STATUS isc_status[20];

	isc_dsql_sql_info(isc_status, &fb_cursor->stmt, sizeof(request), request, sizeof(response), response);
	fb_error_check(isc_status);
	if (response[0] != isc_info_sql_records) { return -1; }

	r = response + 3; /* skip past first cluster */
	while (*r != isc_info_end) {
		char count_type = *r++;
		short len = isc_vax_integer(r, sizeof(short));
		r += sizeof(short);
		switch (count_type) {
			case isc_info_req_insert_count:
				inserted = isc_vax_integer(r, len);
				break;
			case isc_info_req_select_count:
				selected = isc_vax_integer(r, len);
				break;
			case isc_info_req_update_count:
				updated = isc_vax_integer(r, len);
				break;
			case isc_info_req_delete_count:
				deleted = isc_vax_integer(r, len);
				break;
		}
		r += len;
	}
	switch (statement_type) {
		case isc_info_sql_stmt_select: return selected;
		case isc_info_sql_stmt_insert: return inserted;
		case isc_info_sql_stmt_update: return updated;
		case isc_info_sql_stmt_delete: return deleted;
		default: return inserted + selected + updated + deleted;
	}
}

/* call-seq:
 *   execute2(sql, *args) -> nil or rows affected
 *
 * This function is not published.
 */
static VALUE cursor_execute2(VALUE args)
{
	struct FbCursor *fb_cursor;
	struct FbConnection *fb_connection;
	char *sql;
	VALUE rb_sql;
	long statement;
	long length;
	long in_params;
	long cols;
	long rows_affected;
	VALUE result = Qnil;
	char isc_info_buff[16];
	char isc_info_stmt[] = { isc_info_sql_stmt_type };

	VALUE self = rb_ary_pop(args);
	Data_Get_Struct(self, struct FbCursor, fb_cursor);
	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);

	rb_sql = rb_ary_shift(args);
	sql = StringValuePtr(rb_sql);

	/* Prepare query */
	isc_dsql_prepare(fb_connection->isc_status, &fb_connection->transact, &fb_cursor->stmt, 0, sql, fb_connection_dialect(fb_connection), fb_cursor->o_sqlda);
	fb_error_check(fb_connection->isc_status);

	/* Get the statement type */
	isc_dsql_sql_info(fb_connection->isc_status, &fb_cursor->stmt,
			sizeof(isc_info_stmt), isc_info_stmt,
			sizeof(isc_info_buff), isc_info_buff);
	fb_error_check(fb_connection->isc_status);

	if (isc_info_buff[0] == isc_info_sql_stmt_type) {
		length = isc_vax_integer(&isc_info_buff[1], 2);
		statement = isc_vax_integer(&isc_info_buff[3], (short)length);
	} else {
		statement = 0;
	}
	/* Describe the parameters */
	isc_dsql_describe_bind(fb_connection->isc_status, &fb_cursor->stmt, 1, fb_cursor->i_sqlda);
	fb_error_check(fb_connection->isc_status);

	isc_dsql_describe(fb_connection->isc_status, &fb_cursor->stmt, 1, fb_cursor->o_sqlda);
	fb_error_check(fb_connection->isc_status);

	/* Get the number of parameters and reallocate the SQLDA */
	in_params = fb_cursor->i_sqlda->sqld;
	if (fb_cursor->i_sqlda->sqln < in_params) {
		xfree(fb_cursor->i_sqlda);
		fb_cursor->i_sqlda = sqlda_alloc(in_params);
		/* Describe again */
		isc_dsql_describe_bind(fb_connection->isc_status, &fb_cursor->stmt, 1, fb_cursor->i_sqlda);
		fb_error_check(fb_connection->isc_status);
	}

    /* Get the size of parameters buffer and reallocate it */
	if (in_params) {
		length = calculate_buffsize(fb_cursor->i_sqlda);
		if (length > fb_cursor->i_buffer_size) {
			fb_cursor->i_buffer = xrealloc(fb_cursor->i_buffer, length);
			fb_cursor->i_buffer_size = length;
		}
	}

    /* Execute the SQL statement if it is not query */
	if (!fb_cursor->o_sqlda->sqld) {
		if (statement == isc_info_sql_stmt_start_trans) {
			rb_raise(rb_eFbError, "use Fb::Connection#transaction()");
		} else if (statement == isc_info_sql_stmt_commit) {
			rb_raise(rb_eFbError, "use Fb::Connection#commit()");
		} else if (statement == isc_info_sql_stmt_rollback) {
			rb_raise(rb_eFbError, "use Fb::Connection#rollback()");
		} else if (in_params) {
			fb_cursor_execute_withparams(fb_cursor, RARRAY_LEN(args), RARRAY_PTR(args));
		} else {
			isc_dsql_execute2(fb_connection->isc_status, &fb_connection->transact, &fb_cursor->stmt, SQLDA_VERSION1, NULL, NULL);
			fb_error_check(fb_connection->isc_status);
		}
		rows_affected = cursor_rows_affected(fb_cursor, statement);
		result = INT2NUM(rows_affected);
	} else {
		/* Open cursor if the SQL statement is query */
		/* Get the number of columns and reallocate the SQLDA */
		cols = fb_cursor->o_sqlda->sqld;
		if (fb_cursor->o_sqlda->sqln < cols) {
			xfree(fb_cursor->o_sqlda);
			fb_cursor->o_sqlda = sqlda_alloc(cols);
			/* Describe again */
			isc_dsql_describe(fb_connection->isc_status, &fb_cursor->stmt, 1, fb_cursor->o_sqlda);
			fb_error_check(fb_connection->isc_status);
		}

		if (in_params) {
			fb_cursor_set_inputparams(fb_cursor, RARRAY_LEN(args), RARRAY_PTR(args));
		}

		/* Open cursor */
		isc_dsql_execute2(fb_connection->isc_status, &fb_connection->transact, &fb_cursor->stmt, SQLDA_VERSION1, in_params ? fb_cursor->i_sqlda : NULL, NULL);
		fb_error_check(fb_connection->isc_status);
		fb_cursor->open = Qtrue;

		/* Get the size of results buffer and reallocate it */
		length = calculate_buffsize(fb_cursor->o_sqlda);
		if (length > fb_cursor->o_buffer_size) {
			fb_cursor->o_buffer = xrealloc(fb_cursor->o_buffer, length);
			fb_cursor->o_buffer_size = length;
		}

		/* Set the description attributes */
		fb_cursor->fields_ary = fb_cursor_fields_ary(fb_cursor->o_sqlda, fb_connection->downcase_names);
		fb_cursor->fields_hash = fb_cursor_fields_hash(fb_cursor->fields_ary);
	}
	return result;
}

/* call-seq:
 *   execute(sql, *args) -> nil or rows affected
 *
 * This function is no longer published.
 */
static VALUE cursor_execute(int argc, VALUE* argv, VALUE self)
{
	struct FbCursor *fb_cursor;
	struct FbConnection *fb_connection;
	VALUE args;

	if (argc < 1) {
		rb_raise(rb_eArgError, "At least 1 argument required.");
	}

	args = rb_ary_new4(argc, argv);
	rb_ary_push(args, self);

	Data_Get_Struct(self, struct FbCursor, fb_cursor);
	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);
	fb_connection_check(fb_connection);

	if (fb_cursor->open) {
		isc_dsql_free_statement(fb_connection->isc_status, &fb_cursor->stmt, DSQL_close);
		fb_error_check(fb_connection->isc_status);
		fb_cursor->open = Qfalse;
	}

	if (!fb_connection->transact) {
		VALUE result;
		int state;

		fb_connection_transaction_start(fb_connection, Qnil);
		fb_cursor->auto_transact = fb_connection->transact;

		result = rb_protect(cursor_execute2, args, &state);
		if (state) {
			fb_connection_rollback(fb_connection);
			return rb_funcall(rb_mKernel, rb_intern("raise"), 0);
		} else if (result != Qnil) {
			fb_connection_commit(fb_connection);
			return result;
		} else {
			return result;
		}
	} else {
		return cursor_execute2(args);
	}
}

static VALUE fb_hash_from_ary(VALUE fields, VALUE row)
{
	VALUE hash = rb_hash_new();
	int i;
	for (i = 0; i < RARRAY_LEN(fields); i++) {
		VALUE field = rb_ary_entry(fields, i);
		VALUE name = rb_struct_aref(field, LONG2NUM(0));
		VALUE v = rb_ary_entry(row, i);
		rb_hash_aset(hash, name, v);
	}
	return hash;
}

static int hash_format(int argc, VALUE *argv)
{
	if (argc == 0 || argv[0] == ID2SYM(rb_intern("array"))) {
		return 0;
	} else if (argv[0] == ID2SYM(rb_intern("hash"))) {
		return 1;
	} else {
		rb_raise(rb_eFbError, "Unknown format");
	}
}

/* call-seq:
 *   fetch() -> Array
 *   fetch(:array) -> Array
 *   fetch(:hash) -> Hash
 *
 * Reads and returns a single row from the open cursor in either an Array or a Hash,
 * where the column names or aliases from the query form the keys.
 * If the +downcase_names+ attribute of the associated connection evaluates to true,
 * the keys are lower case, except where the column name was mixed case to begin with.
 */
static VALUE cursor_fetch(int argc, VALUE* argv, VALUE self)
{
	VALUE ary;
	struct FbCursor *fb_cursor;

	int hash_row = hash_format(argc, argv);

	Data_Get_Struct(self, struct FbCursor, fb_cursor);
	fb_cursor_fetch_prep(fb_cursor);

	ary = fb_cursor_fetch(fb_cursor);
	if (NIL_P(ary)) return Qnil;
	return hash_row ? fb_hash_from_ary(fb_cursor->fields_ary, ary) : ary;
}

/* call-seq:
 *   fetchall() -> Array of Arrays
 *   fetchall(:array) -> Array of Arrays
 *   fetchall(:hash) -> Array of Hashes
 *
 * Returns the remainder of the rows from the open cursor, with each row represented
 * by either an Array or a Hash, where the column names or aliases from the query form the keys.
 * If the +downcase_names+ attribute of the associated connection evaluates to true,
 * the keys are lower case, except where the column name was mixed case to begin with.
 */
static VALUE cursor_fetchall(int argc, VALUE* argv, VALUE self)
{
	VALUE ary, row;
	struct FbCursor *fb_cursor;

	int hash_rows = hash_format(argc, argv);

	Data_Get_Struct(self, struct FbCursor, fb_cursor);
	fb_cursor_fetch_prep(fb_cursor);

	ary = rb_ary_new();
	for (;;) {
		row = fb_cursor_fetch(fb_cursor);
		if (NIL_P(row)) break;
		if (hash_rows) {
			rb_ary_push(ary, fb_hash_from_ary(fb_cursor->fields_ary, row));
		} else {
			rb_ary_push(ary, row);
		}
	}

	return ary;
}

/* call-seq:
 *   each() {|Array| } -> nil
 *   each(:array) {|Array| } -> nil
 *   each(:hash) {|Hash| } -> nil
 *
 * Iterates the rows from the open cursor, passing each one to a block in either
 * an Array or a Hash, where the column names or aliases from the query form the keys.
 * If the +downcase_names+ attribute of the associated connection evaluates to true,
 * the keys are lower case, except where the column name was mixed case to begin with.
 */
static VALUE cursor_each(int argc, VALUE* argv, VALUE self)
{
	VALUE row;
	struct FbCursor *fb_cursor;

	int hash_rows = hash_format(argc, argv);

	Data_Get_Struct(self, struct FbCursor, fb_cursor);
	fb_cursor_fetch_prep(fb_cursor);

	for (;;) {
		row = fb_cursor_fetch(fb_cursor);
		if (NIL_P(row)) break;
		if (hash_rows) {
			rb_yield(fb_hash_from_ary(fb_cursor->fields_ary, row));
		} else {
			rb_yield(row);
		}
	}

	return Qnil;
}

/* call-seq:
 *   close(sql, *args) -> nil
 *
 * Closes the cursor.  If a transaction was automatically started for this cursor, the transaction is commited.
 */
static VALUE cursor_close(VALUE self)
{
	struct FbCursor *fb_cursor;
	struct FbConnection *fb_connection;

	Data_Get_Struct(self, struct FbCursor, fb_cursor);
	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);
	fb_cursor_check(fb_cursor);

	/* Close the cursor */
	if (fb_cursor->stmt) {
		isc_dsql_free_statement(fb_connection->isc_status, &fb_cursor->stmt, DSQL_close);
		fb_error_check_warn(fb_connection->isc_status);
		isc_dsql_free_statement(fb_connection->isc_status, &fb_cursor->stmt, DSQL_drop);
		fb_error_check(fb_connection->isc_status);
		fb_cursor->open = Qfalse;
		if (fb_connection->transact == fb_cursor->auto_transact) {
			isc_commit_transaction(fb_connection->isc_status, &fb_connection->transact);
			fb_cursor->auto_transact = fb_connection->transact;
			fb_error_check(fb_connection->isc_status);
		}
	}
	fb_cursor->fields_ary = Qnil;
	fb_cursor->fields_hash = Qnil;
	return Qnil;
}

/* call-seq:
 *   drop() -> nil
 *
 * Drops the cursor.
 *
 * TODO: How is this different from close()?
 */
static VALUE cursor_drop(VALUE self)
{
	struct FbCursor *fb_cursor;
	struct FbConnection *fb_connection;
	int i;

	Data_Get_Struct(self, struct FbCursor, fb_cursor);
	fb_cursor_drop(fb_cursor);
	fb_cursor->fields_ary = Qnil;
	fb_cursor->fields_hash = Qnil;

	/* reset the reference from connection */
	Data_Get_Struct(fb_cursor->connection, struct FbConnection, fb_connection);
	for (i = 0; i < RARRAY_LEN(fb_connection->cursor); i++) {
		if (RARRAY_PTR(fb_connection->cursor)[i] == self) {
			RARRAY_PTR(fb_connection->cursor)[i] = Qnil;
		}
	}

	return Qnil;
}

/* call-seq:
 *   fields() -> Array
 *   fields(:array) -> Array
 *   fields(:hash) -> Hash
 *
 * Return an array of Field Structs or a hash indexed by field name.
 * If the +downcase_names+ attribute of the associated connection evaluates to true,
 * the keys are lower case, except where the column name was mixed case to begin with.
 */
static VALUE cursor_fields(int argc, VALUE* argv, VALUE self)
{
	struct FbCursor *fb_cursor;

	Data_Get_Struct(self, struct FbCursor, fb_cursor);
	if (argc == 0 || argv[0] == ID2SYM(rb_intern("array"))) {
		return fb_cursor->fields_ary;
	} else if (argv[0] == ID2SYM(rb_intern("hash"))) {
		return fb_cursor->fields_hash;
	} else {
		rb_raise(rb_eFbError, "Unknown format");
	}
}

/* call-seq:
 *   error_code -> int
 *
 * Returns the sqlcode associated with the error.
 */
static VALUE error_error_code(VALUE error)
{
	rb_p(error);
	return rb_iv_get(error, "error_code");
}

static char* dbp_create(long *length)
{
	char *dbp = ALLOC_N(char, 1);
	*dbp = isc_dpb_version1;
	*length = 1;
	return dbp;
}

static char* dbp_add_string(char *dbp, char isc_dbp_code, char *s, long *length)
{
	char *buf;
	long old_length = *length;
	long s_len = strlen(s);
	*length += 2 + s_len;
	REALLOC_N(dbp, char, *length);
	buf = dbp + old_length;
	*buf++ = isc_dbp_code;
	*buf++ = (char)s_len;
	memcpy(buf, s, s_len);
	return dbp;
}

static char* connection_create_dbp(VALUE self, long *length)
{
	char *dbp;
	VALUE username, password, charset, role;

	username = rb_iv_get(self, "@username");
	Check_Type(username, T_STRING);
	password = rb_iv_get(self, "@password");
	Check_Type(password, T_STRING);
	role = rb_iv_get(self, "@role");
	charset = rb_iv_get(self, "@charset");

	dbp = dbp_create(length);
	dbp = dbp_add_string(dbp, isc_dpb_user_name, StringValuePtr(username), length);
	dbp = dbp_add_string(dbp, isc_dpb_password, StringValuePtr(password), length);
	if (!NIL_P(charset)) {
		dbp = dbp_add_string(dbp, isc_dpb_lc_ctype, StringValuePtr(charset), length);
	}
	if (!NIL_P(role)) {
		dbp = dbp_add_string(dbp, isc_dpb_sql_role_name, StringValuePtr(role), length);
	}
	return dbp;
}

static const char* CONNECTION_PARMS[] = {
	"@database",
	"@username",
	"@password",
	"@charset",
	"@role",
	"@downcase_names",
	"@encoding",
	(char *)0
};

static VALUE connection_create(isc_db_handle handle, VALUE db)
{
	unsigned short dialect;
	unsigned short db_dialect;
	VALUE downcase_names;
	const char *parm;
	int i;
	struct FbConnection *fb_connection;
	VALUE connection = Data_Make_Struct(rb_cFbConnection, struct FbConnection, fb_connection_mark, fb_connection_free, fb_connection);
	fb_connection->db = handle;
	fb_connection->transact = 0;
	fb_connection->cursor = rb_ary_new();
	dialect = SQL_DIALECT_CURRENT;
	db_dialect = fb_connection_db_SQL_Dialect(fb_connection);

	if (db_dialect < dialect) {
		dialect = db_dialect;
		/* TODO: downgrade warning */
	}

	fb_connection->dialect = dialect;
	fb_connection->db_dialect = db_dialect;
	downcase_names = rb_iv_get(db, "@downcase_names");
	fb_connection->downcase_names = RTEST(downcase_names);
	fb_connection->encoding = rb_iv_get(db, "@encoding");

	for (i = 0; (parm = CONNECTION_PARMS[i]); i++) {
		rb_iv_set(connection, parm, rb_iv_get(db, parm));
	}

	return connection;
}

static VALUE connection_names(VALUE self, const char *sql)
{
	VALUE row;
	VALUE query = rb_str_new2(sql);
	VALUE cursor = connection_execute(1, &query, self);
	VALUE names = rb_ary_new();
	struct FbConnection *fb_connection;
	Data_Get_Struct(self, struct FbConnection, fb_connection);

	while ((row = cursor_fetch(0, NULL, cursor)) != Qnil) {
		VALUE name = rb_ary_entry(row, 0);
		if (fb_connection->downcase_names && no_lowercase(name)) {
			rb_funcall(name, id_downcase_bang, 0);
		}
		rb_funcall(name, id_rstrip_bang, 0);
		rb_ary_push(names, name);
	}

	cursor_close(cursor);
	return names;
}

/* call-seq:
 *   table_names() -> array
 *
 * Returns sorted array of table names in connected database.
 */
static VALUE connection_table_names(VALUE self)
{
	const char *sql = "SELECT RDB$RELATION_NAME FROM RDB$RELATIONS "
				"WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND RDB$VIEW_BLR IS NULL "
				"ORDER BY RDB$RELATION_NAME";
	return connection_names(self, sql);
}

/* call-seq:
 *   generator_names() -> array
 *
 * Returns sorted array of generator names in connected database.
 */
static VALUE connection_generator_names(VALUE self)
{
	const char *sql = "SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS "
				"WHERE (RDB$SYSTEM_FLAG IS NULL OR RDB$SYSTEM_FLAG <> 1) "
				"ORDER BY RDB$GENERATOR_NAME";
	return connection_names(self, sql);
}

/* call-seq:
 *   view_names() -> array
 *
 * Returns sorted array of view names in connected database.
 */
static VALUE connection_view_names(VALUE self)
{
	const char *sql = "SELECT RDB$RELATION_NAME, RDB$OWNER_NAME, RDB$VIEW_SOURCE FROM RDB$RELATIONS "
				"WHERE (RDB$SYSTEM_FLAG <> 1 OR RDB$SYSTEM_FLAG IS NULL) AND NOT RDB$VIEW_BLR IS NULL AND RDB$FLAGS = 1 "
				"ORDER BY RDB$RELATION_ID";
	return connection_names(self, sql);
}

/* call-seq:
 *   role_names() -> array
 *
 * Returns sorted array of role names in connected database.
 */
static VALUE connection_role_names(VALUE self)
{
	const char *sql = "SELECT * FROM RDB$ROLES WHERE RDB$SYSTEM_FLAG = 0 ORDER BY RDB$ROLE_NAME";
	return connection_names(self, sql);
}

/* call-seq:
 *   procedure_names() -> array
 *
 * Returns sorted array of stored procedure names in connected database.
 */
static VALUE connection_procedure_names(VALUE self)
{
	const char *sql = "SELECT RDB$PROCEDURE_NAME FROM RDB$PROCEDURES "
				"ORDER BY RDB$PROCEDURE_NAME";
	return connection_names(self, sql);
}

/* call-seq:
 *   trigger_names() -> array
 *
 * Returns sorted array of trigger names in connected database.
 */
static VALUE connection_trigger_names(VALUE self)
{
	const char *sql = "SELECT RDB$TRIGGER_NAME FROM RDB$TRIGGERS "
				"ORDER BY RDB$TRIGGER_NAME";
	return connection_names(self, sql);
}

/* call-seq:
 *   columns(table_name) -> array
 *
 * Returns array of objects describing each column of table_name.
 */

static VALUE connection_columns(VALUE self, VALUE table_name)
{
    int i;
    struct FbConnection *fb_connection;
    VALUE re_default = rb_reg_new("^\\s*DEFAULT\\s+", strlen("^\\s*DEFAULT\\s+"), IGNORECASE);
    VALUE re_rdb = rb_reg_new("^RDB\\$", strlen("^RDB\\$"), 0);
    VALUE empty = rb_str_new(NULL, 0);
    VALUE columns = rb_ary_new();
    const char *sql = "SELECT r.rdb$field_name NAME, r.rdb$field_source, f.rdb$field_type, f.rdb$field_sub_type, "
                "f.rdb$field_length, f.rdb$field_precision, f.rdb$field_scale SCALE, "
                "COALESCE(r.rdb$default_source, f.rdb$default_source), "
                "COALESCE(r.rdb$null_flag, f.rdb$null_flag) "
                "FROM rdb$relation_fields r "
                "JOIN rdb$fields f ON r.rdb$field_source = f.rdb$field_name "
                "WHERE UPPER(r.rdb$relation_name) = ? "
                "ORDER BY r.rdb$field_position";
    VALUE query = rb_str_new2(sql);
    VALUE upcase_table_name = rb_funcall(table_name, rb_intern("upcase"), 0);
    VALUE query_parms[] = { query, upcase_table_name };
    VALUE rs = connection_query(2, query_parms, self);
    Data_Get_Struct(self, struct FbConnection, fb_connection);
    for (i = 0; i < RARRAY_LEN(rs); i++) {
        VALUE row = rb_ary_entry(rs, i);
        VALUE name = rb_ary_entry(row, 0);
        VALUE domain = rb_ary_entry(row, 1);
        VALUE sql_type = rb_ary_entry(row, 2);
        VALUE sql_subtype = rb_ary_entry(row, 3);
        VALUE length = rb_ary_entry(row, 4);
        VALUE precision = rb_ary_entry(row, 5);
        VALUE scale = rb_ary_entry(row, 6);
        VALUE dflt = rb_ary_entry(row, 7);
        VALUE not_null = rb_ary_entry(row, 8);
        VALUE nullable;
        VALUE column;
        rb_funcall(name, id_rstrip_bang, 0);
        rb_funcall(domain, id_rstrip_bang, 0);
        if (fb_connection->downcase_names && no_lowercase(name)) {
          rb_funcall(name, id_downcase_bang, 0);
        }
        if (rb_funcall(re_rdb, rb_intern("match"), 1, domain) != Qnil) {
            domain = Qnil;
        }
        if (sql_subtype == Qnil) {
            sql_subtype = INT2NUM(0);
        }
        sql_type = sql_type_from_code(self, sql_type, sql_subtype);
        if (dflt != Qnil) {
            rb_funcall(dflt, id_sub_bang, 2, re_default, empty);
        }
        nullable = RTEST(not_null) ? Qfalse : Qtrue;
        column = rb_struct_new(rb_sFbColumn, name, domain, sql_type, sql_subtype, length, precision, scale, dflt, nullable);
        rb_ary_push(columns, column);
    }
    rb_ary_freeze(columns);
    return columns;
}

char *p(char *prompt, VALUE s)
{
	char *sz;
	if (TYPE(s) != T_STRING) {
		s = rb_funcall(s, rb_intern("to_s"), 0);
	}
	sz = StringValuePtr(s);
	printf("%s: %s\n", prompt, sz);
	return sz;
}

static VALUE connection_index_columns(VALUE self, VALUE index_name)
{
	const char *sql_columns = "SELECT * "
						"FROM RDB$INDEX_SEGMENTS "
						"WHERE RDB$INDEX_SEGMENTS.RDB$INDEX_NAME = ? "
						"ORDER BY RDB$INDEX_SEGMENTS.RDB$FIELD_POSITION";
	VALUE query_columns = rb_str_new2(sql_columns);
	VALUE query_parms[] = { query_columns, index_name };
	VALUE result = connection_query(2, query_parms, self);
	VALUE columns = rb_ary_new();
	int i;
	struct FbConnection *fb_connection;
	Data_Get_Struct(self, struct FbConnection, fb_connection);

	for (i = 0; i < RARRAY_LEN(result); i++) {
		VALUE row = rb_ary_entry(result, i);
		VALUE name = rb_ary_entry(row, 1);
		rb_funcall(name, id_rstrip_bang, 0);
		if (fb_connection->downcase_names && no_lowercase(name)) {
			rb_funcall(name, id_downcase_bang, 0);
		}
		rb_ary_push(columns, name);
	}
	return columns;
}

/* call-seq:
 *   indexes() -> Hash
 *
 * Returns a hash of indexes, keyed by index name.
 */
static VALUE connection_indexes(VALUE self)
{
	const char *sql_indexes = "SELECT RDB$INDICES.RDB$RELATION_NAME, RDB$INDICES.RDB$INDEX_NAME, RDB$INDICES.RDB$UNIQUE_FLAG, RDB$INDICES.RDB$INDEX_TYPE "
						"FROM RDB$INDICES "
						"  JOIN RDB$RELATIONS ON RDB$INDICES.RDB$RELATION_NAME = RDB$RELATIONS.RDB$RELATION_NAME "
						"WHERE (RDB$RELATIONS.RDB$SYSTEM_FLAG <> 1 OR RDB$RELATIONS.RDB$SYSTEM_FLAG IS NULL) ";
	VALUE query_indexes = rb_str_new2(sql_indexes);
	VALUE ary_indexes = connection_query(1, &query_indexes, self);
	VALUE indexes = rb_hash_new();
	int i;
	struct FbConnection *fb_connection;
	Data_Get_Struct(self, struct FbConnection, fb_connection);

	for (i = 0; i < RARRAY_LEN(ary_indexes); i++) {
		VALUE index_struct;
		VALUE row = rb_ary_entry(ary_indexes, i);
		VALUE table_name = rb_ary_entry(row, 0);
		VALUE index_name = rb_ary_entry(row, 1);
		VALUE unique = rb_ary_entry(row, 2);
		VALUE descending = rb_ary_entry(row, 3);
		VALUE columns = connection_index_columns(self, index_name);

		rb_funcall(table_name, id_rstrip_bang, 0);
		rb_funcall(index_name, id_rstrip_bang, 0);

		if (fb_connection->downcase_names) {
			if (no_lowercase(table_name)) {
				rb_funcall(table_name, id_downcase_bang, 0);
			}
			if (no_lowercase(index_name)) {
				rb_funcall(index_name, id_downcase_bang, 0);
			}
		}

		rb_str_freeze(table_name);
		rb_str_freeze(index_name);

		unique = (unique == INT2FIX(1)) ? Qtrue : Qfalse;
		descending = (descending == INT2FIX(1)) ? Qtrue : Qfalse;

		index_struct = rb_struct_new(rb_sFbIndex, table_name, index_name, unique, descending, columns);
		rb_hash_aset(indexes, index_name, index_struct);
	}
	return indexes;
}

/*
static void define_attrs(VALUE klass, char **attrs)
{
	char *parm;
	while ((parm = *attrs))
	{
		rb_define_attr(klass, parm+1, 1, 1);
		attrs++;
	}
}
*/

static VALUE default_string(VALUE hash, const char *key, const char *def)
{
	VALUE sym = ID2SYM(rb_intern(key));
	VALUE val = rb_hash_aref(hash, sym);
	return NIL_P(val) ? rb_str_new2(def) : StringValue(val);
}

static VALUE default_int(VALUE hash, const char *key, int def)
{
	VALUE sym = ID2SYM(rb_intern(key));
	VALUE val = rb_hash_aref(hash, sym);
	return NIL_P(val) ? INT2NUM(def) : val;
}

static VALUE database_allocate_instance(VALUE klass)
{
	NEWOBJ(obj, struct RObject);
	OBJSETUP((VALUE)obj, klass, T_OBJECT);
	return (VALUE)obj;
}

static VALUE hash_from_connection_string(VALUE cs)
{
	VALUE hash = rb_hash_new();
	VALUE re_SemiColon = rb_reg_regcomp(rb_str_new2("\\s*;\\s*"));
	VALUE re_Equal = rb_reg_regcomp(rb_str_new2("\\s*=\\s*"));
	ID id_split = rb_intern("split");
	VALUE pairs = rb_funcall(cs, id_split, 1, re_SemiColon);
	int i;
	for (i = 0; i < RARRAY_LEN(pairs); i++) {
		VALUE pair = rb_ary_entry(pairs, i);
		VALUE keyValue = rb_funcall(pair, id_split, 1, re_Equal);
		if (RARRAY_LEN(keyValue) == 2) {
			VALUE key = rb_ary_entry(keyValue, 0);
			VALUE val = rb_ary_entry(keyValue, 1);
			rb_hash_aset(hash, rb_str_intern(key), val);
		}
	}
	return hash;
}

static void check_page_size(int page_size)
{
	if (page_size != 1024 && page_size != 2048 && page_size != 4096 && page_size != 8192 && page_size != 16384) {
		rb_raise(rb_eFbError, "Invalid page size: %d", page_size);
	}
}

/* call-seq:
 *   Database.new(options) -> Database
 *
 * Initialize Database with Hash of values:
 * :database:: Full Firebird connection string, e.g. 'localhost:/var/fbdata/drivertest.fdb' (required)
 * :username:: database username (default: 'sysdba')
 * :password:: database password (default: 'masterkey')
 * :charset:: character set to be used with the connection (default: 'NONE')
 * :role:: database role to connect using (default: nil)
 * :downcase_names:: Column names are reported in lowercase, unless they were originally mixed case (default: nil).
 * :page_size:: page size to use when creating a database (default: 4096)
 */
static VALUE database_initialize(int argc, VALUE *argv, VALUE self)
{
	VALUE parms, database;

	if (argc >= 1) {
		parms = argv[0];
		if (TYPE(parms) == T_STRING) {
			parms = hash_from_connection_string(parms);
		} else {
			Check_Type(parms, T_HASH);
		}
		database = rb_hash_aref(parms, ID2SYM(rb_intern("database")));
		if (NIL_P(database)) rb_raise(rb_eFbError, "Database must be specified.");
		rb_iv_set(self, "@database", database);
		rb_iv_set(self, "@username", default_string(parms, "username", "sysdba"));
		rb_iv_set(self, "@password", default_string(parms, "password", "masterkey"));
		rb_iv_set(self, "@charset", default_string(parms, "charset", "NONE"));
		rb_iv_set(self, "@role", rb_hash_aref(parms, ID2SYM(rb_intern("role"))));
		rb_iv_set(self, "@downcase_names", rb_hash_aref(parms, ID2SYM(rb_intern("downcase_names"))));
		rb_iv_set(self, "@encoding", default_string(parms, "encoding", "ASCII-8BIT"));
		rb_iv_set(self, "@page_size", default_int(parms, "page_size", 4096));
	}
	return self;
}

/* call-seq:
 *   create() -> Database
 *   create() {|connection| } -> Database
 *
 * Create a database using the current database options.
 * If a block is provided, an open connection to the new database is passed to it
 * before being automatically closed.
 */
static VALUE database_create(VALUE self)
{
	ISC_STATUS isc_status[20];
	isc_db_handle handle = 0;
	isc_tr_handle local_transact = 0;
	VALUE parms, fmt, stmt;
	char *sql;

	VALUE database = rb_iv_get(self, "@database");
	VALUE username = rb_iv_get(self, "@username");
	VALUE password = rb_iv_get(self, "@password");
	VALUE page_size = rb_iv_get(self, "@page_size");
	VALUE charset = rb_iv_get(self, "@charset");

	check_page_size(NUM2INT(page_size));

	parms = rb_ary_new3(5, database, username, password, page_size, charset);

	fmt = rb_str_new2("CREATE DATABASE '%s' USER '%s' PASSWORD '%s' PAGE_SIZE = %d DEFAULT CHARACTER SET %s;");
	stmt = rb_funcall(fmt, rb_intern("%"), 1, parms);
	sql = StringValuePtr(stmt);

	if (isc_dsql_execute_immediate(isc_status, &handle, &local_transact, 0, sql, 3, NULL) != 0) {
		fb_error_check(isc_status);
	}
	if (handle) {
		if (rb_block_given_p()) {
			VALUE connection = connection_create(handle, self);
			rb_ensure(rb_yield,connection,connection_close,connection);
		} else {
			isc_detach_database(isc_status, &handle);
			fb_error_check(isc_status);
		}
	}

	return self;
}

/* call-seq:
 *   Database.create(options) -> Database
 *   Database.create(options) {|connection| } -> Database
 *
 * Create a database using the specified options (see: Database.new for details of options Hash).
 * If a block is provided, an open connection to the new database is passed to it
 * before being automatically closed.
 */
static VALUE database_s_create(int argc, VALUE *argv, VALUE klass)
{
	VALUE obj = database_allocate_instance(klass);
	database_initialize(argc, argv, obj);
	return database_create(obj);
}

/* call-seq:
 *   connect() -> Connection
 *   connect() {|connection| } -> nil
 *
 * Connect to the database specified by the current database options.
 *
 * If a block is provided, the open connection is passed to it before being
 * automatically closed.
 */
static VALUE database_connect(VALUE self)
{
	ISC_STATUS isc_status[20];
	char *dbp;
	long length;
	isc_db_handle handle = 0;
	VALUE database = rb_iv_get(self, "@database");

	Check_Type(database, T_STRING);
	dbp = connection_create_dbp(self, &length);
	isc_attach_database(isc_status, 0, StringValuePtr(database), &handle, length, dbp);
	xfree(dbp);
	fb_error_check(isc_status);
	{
		VALUE connection = connection_create(handle, self);
		if (rb_block_given_p()) {
			return rb_ensure(rb_yield, connection, connection_close, connection);
			return Qnil;
		} else {
			return connection;
		}
	}
}

/* call-seq:
 *   Database.connect(options) -> Connection
 *   Database.connect(options) {|connection| } -> nil
 *
 * Connect to a database using the options given (see: Database.new for details of options Hash).
 *
 * If a block is provided, the open connection is passed to it before being
 * automatically closed.
 */
static VALUE database_s_connect(int argc, VALUE *argv, VALUE klass)
{
	VALUE obj = database_allocate_instance(klass);
	database_initialize(argc, argv, obj);
	return database_connect(obj);
}

/* call-seq:
 *   drop() -> nil
 *
 * Drop the database specified by the current database options.
 */
static VALUE database_drop(VALUE self)
{
	struct FbConnection *fb_connection;

	VALUE connection = database_connect(self);
	Data_Get_Struct(connection, struct FbConnection, fb_connection);
	isc_drop_database(fb_connection->isc_status, &fb_connection->db);
	fb_error_check(fb_connection->isc_status);
	/* fb_connection_remove(fb_connection); */
	return Qnil;
}

/* call-seq:
 *   Database.drop(options) -> nil
 *
 * Drop the database specified by the options given (see: Database.new for details of options Hash).
 */
static VALUE database_s_drop(int argc, VALUE *argv, VALUE klass)
{
	VALUE obj = database_allocate_instance(klass);
	database_initialize(argc, argv, obj);
	return database_drop(obj);
}

void Init_fb()
{
	rb_funcall(rb_mKernel, rb_intern("require"), 1, rb_str_new2("bigdecimal"));

	rb_mFb = rb_define_module("Fb");

	rb_cFbDatabase = rb_define_class_under(rb_mFb, "Database", rb_cData);
    rb_define_alloc_func(rb_cFbDatabase, database_allocate_instance);
    rb_define_method(rb_cFbDatabase, "initialize", database_initialize, -1);
	rb_define_attr(rb_cFbDatabase, "database", 1, 1);
	rb_define_attr(rb_cFbDatabase, "username", 1, 1);
	rb_define_attr(rb_cFbDatabase, "password", 1, 1);
	rb_define_attr(rb_cFbDatabase, "charset", 1, 1);
	rb_define_attr(rb_cFbDatabase, "role", 1, 1);
	rb_define_attr(rb_cFbDatabase, "downcase_names", 1, 1);
	rb_define_attr(rb_cFbDatabase, "encoding", 1, 1);
	rb_define_attr(rb_cFbDatabase, "page_size", 1, 1);
    rb_define_method(rb_cFbDatabase, "create", database_create, 0);
	rb_define_singleton_method(rb_cFbDatabase, "create", database_s_create, -1);
	rb_define_method(rb_cFbDatabase, "connect", database_connect, 0);
	rb_define_singleton_method(rb_cFbDatabase, "connect", database_s_connect, -1);
	rb_define_method(rb_cFbDatabase, "drop", database_drop, 0);
	rb_define_singleton_method(rb_cFbDatabase, "drop", database_s_drop, -1);

	rb_cFbConnection = rb_define_class_under(rb_mFb, "Connection", rb_cData);
	rb_define_attr(rb_cFbConnection, "database", 1, 1);
	rb_define_attr(rb_cFbConnection, "username", 1, 1);
	rb_define_attr(rb_cFbConnection, "password", 1, 1);
	rb_define_attr(rb_cFbConnection, "charset", 1, 1);
	rb_define_attr(rb_cFbConnection, "role", 1, 1);
	rb_define_attr(rb_cFbConnection, "downcase_names", 1, 1);
	rb_define_attr(rb_cFbConnection, "encoding", 1, 1);
	rb_define_method(rb_cFbConnection, "to_s", connection_to_s, 0);
	rb_define_method(rb_cFbConnection, "execute", connection_execute, -1);
	rb_define_method(rb_cFbConnection, "query", connection_query, -1);
	rb_define_method(rb_cFbConnection, "transaction", connection_transaction, -1);
	rb_define_method(rb_cFbConnection, "transaction_started", connection_transaction_started, 0);
	rb_define_method(rb_cFbConnection, "commit", connection_commit, 0);
	rb_define_method(rb_cFbConnection, "rollback", connection_rollback, 0);
	rb_define_method(rb_cFbConnection, "close", connection_close, 0);
	rb_define_method(rb_cFbConnection, "drop", connection_drop, 0);
	rb_define_method(rb_cFbConnection, "open?", connection_is_open, 0);
	rb_define_method(rb_cFbConnection, "dialect", connection_dialect, 0);
	rb_define_method(rb_cFbConnection, "db_dialect", connection_db_dialect, 0);
	rb_define_method(rb_cFbConnection, "table_names", connection_table_names, 0);
	rb_define_method(rb_cFbConnection, "generator_names", connection_generator_names, 0);
	rb_define_method(rb_cFbConnection, "view_names", connection_view_names, 0);
	rb_define_method(rb_cFbConnection, "role_names", connection_role_names, 0);
	rb_define_method(rb_cFbConnection, "procedure_names", connection_procedure_names, 0);
	rb_define_method(rb_cFbConnection, "trigger_names", connection_trigger_names, 0);
	rb_define_method(rb_cFbConnection, "indexes", connection_indexes, 0);
	rb_define_method(rb_cFbConnection, "columns", connection_columns, 1);
	/* rb_define_method(rb_cFbConnection, "cursor", connection_cursor, 0); */

	rb_cFbCursor = rb_define_class_under(rb_mFb, "Cursor", rb_cData);
	/* rb_define_method(rb_cFbCursor, "execute", cursor_execute, -1); */
	rb_define_method(rb_cFbCursor, "fields", cursor_fields, -1);
	rb_define_method(rb_cFbCursor, "fetch", cursor_fetch, -1);
	rb_define_method(rb_cFbCursor, "fetchall", cursor_fetchall, -1);
	rb_define_method(rb_cFbCursor, "each", cursor_each, -1);
	rb_define_method(rb_cFbCursor, "close", cursor_close, 0);
	rb_define_method(rb_cFbCursor, "drop", cursor_drop, 0);

	rb_cFbSqlType = rb_define_class_under(rb_mFb, "SqlType", rb_cData);
	rb_define_singleton_method(rb_cFbSqlType, "from_code", sql_type_from_code, 2);

	rb_eFbError = rb_define_class_under(rb_mFb, "Error", rb_eStandardError);
	rb_define_method(rb_eFbError, "error_code", error_error_code, 0);

	rb_sFbField = rb_struct_define("FbField", "name", "sql_type", "sql_subtype", "display_size", "internal_size", "precision", "scale", "nullable", "type_code", NULL);
	rb_sFbIndex = rb_struct_define("FbIndex", "table_name", "index_name", "unique", "descending", "columns", NULL);
	rb_sFbColumn = rb_struct_define("FbColumn", "name", "domain", "sql_type", "sql_subtype", "length", "precision", "scale", "default", "nullable", NULL);

	rb_require("date");
	rb_require("time"); /* Needed as of Ruby 1.8.5 */
	rb_cDate = rb_const_get(rb_cObject, rb_intern("Date"));

	id_matches = rb_intern("=~");
	id_downcase_bang = rb_intern("downcase!");
	re_lowercase = rb_reg_regcomp(rb_str_new2("[[:lower:]]"));
	rb_global_variable(&re_lowercase);
	id_rstrip_bang = rb_intern("rstrip!");
    id_sub_bang = rb_intern("sub!");
    id_force_encoding = rb_intern("force_encoding");
}
