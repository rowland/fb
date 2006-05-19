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
#include <stdio.h>
#include <string.h>
#include <limits.h>
#include <ibase.h>
#include <float.h>
#include <time.h>

#define	SQLDA_COLSINIT	10
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
static VALUE rb_cFbConnection;
static VALUE rb_cFbCursor;
static VALUE rb_cFbBlob;
static VALUE rb_eFbError;

static long isc_status[20];	/* status vector */
static char isc_info_stmt[] = { isc_info_sql_stmt_type };
static char isc_info_buff[16];
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
} ISC_TEB ;

/* InterBase varchar structure */
typedef	struct
{
	short vary_length;
	char  vary_string[1];
} VARY;

struct IBconn {
	isc_db_handle db;		/* DB handle */
	VALUE curs;
	unsigned short dialect;
	unsigned short db_dialect;
	struct IBconn *next;
} *ibconn_list;

struct IBcurs {
	int open;
	isc_stmt_handle stmt;
	VALUE describe;
	VALUE conn;
};

typedef struct trans_opts
{
	char *option1;
	char *option2;
	char  optval;
	short position;
	struct trans_opts *sub_opts;
} trans_opts;

/* global data */
static isc_tr_handle transact = 0;	/* transaction handle */
static XSQLDA *i_sqlda = 0;
static XSQLDA *o_sqlda = 0;
static char *results = 0;
static long  ressize = 0;
static char *paramts = 0;
static long  prmsize = 0;
static int db_num = 0;

/* global utilities */

#define	ALIGN(n, b)	((n + b - 1) & ~(b - 1))
#define	UPPER(c)	(((c) >= 'a' && (c)<= 'z') ? (c) - 'a' + 'A' : (c))
#define	FREE(p)		if (p)	{ free(p); p = 0; }
#define	SETNULL(p)	if (p && strlen(p) == 0)	{ p = 0; }

static long
calculate_buffsize(sqlda)
    XSQLDA *sqlda;
{
	XSQLVAR *var;
	long cols;
	short dtp;
	long offset;
	long alignment;
	long length;
	long count;

	cols = sqlda->sqld;
	for (var = sqlda->sqlvar, offset = 0,count = 0; count < cols; var++,count++) {
		length = alignment = var->sqllen;
		dtp = var->sqltype & ~1;

		if (dtp == SQL_TEXT)
			alignment = 1;
		else if (dtp == SQL_VARYING) {
			length += sizeof(short);
			alignment = sizeof(short);
	}

	offset = ALIGN(offset, alignment);
	offset += length;
	offset = ALIGN(offset, sizeof(short));
	offset += sizeof(short);
	}

	return offset;
}

static void
ib_error_check()
{
	short code = isc_sqlcode(isc_status);

	if (code != 0) {
		char buf[1024];
		VALUE exc;

		isc_sql_interprete(code, buf, 1024);
		exc = rb_exc_new2(rb_eFbError, buf);
		rb_iv_set(exc, "error_code", INT2FIX(code));
		rb_exc_raise(exc);
	}
}

static void
ib_error_check_warn()
{
	short code = isc_sqlcode(isc_status);
	if (code != 0) {
		char buf[1024];
		isc_sql_interprete(code, buf, 1024);
		rb_warning("%s(%d)", buf, code);
	}
}

static XSQLDA*
sqlda_alloc(cols)
	long cols;
{
	XSQLDA *sqlda;

	sqlda = (XSQLDA*)xmalloc(XSQLDA_LENGTH(cols));
#ifdef SQLDA_CURRENT_VERSION
	sqlda->version = SQLDA_CURRENT_VERSION;
#else
	sqlda->version = SQLDA_VERSION1;
#endif
	sqlda->sqln = cols;
	sqlda->sqld = cols;
	return sqlda;
}

static VALUE ibconn_s_new _((int, VALUE*, VALUE));
static VALUE ibconn_close (VALUE);
static VALUE ibcurs_close _((VALUE));
static VALUE ibcurs_drop _((VALUE));
static VALUE ibcurs_execute _((int, VALUE*, VALUE));

static void curs_mark();
static void curs_free();

/* connection utilities */
static void
conn_check(conn)
	struct IBconn *conn;
{
	if (conn->db == 0) {
		rb_raise(rb_eFbError, "closed db connection");
	}
}

static void
conn_close_cursors()
{
	struct IBconn *list = ibconn_list;
	int i;

	while (list) {
		for (i=0; i<RARRAY(list->curs)->len; i++) {
			ibcurs_close(RARRAY(list->curs)->ptr[i]);
		}
		list = list->next;
	}
}

static void
conn_drop_cursors(conn)
	struct IBconn *conn;
{
	int i;

	for (i=0; i<RARRAY(conn->curs)->len; i++) {
		ibcurs_drop(RARRAY(conn->curs)->ptr[i]);
	}
	RARRAY(conn->curs)->len = 0;
}

static void
conn_remove(conn)
	struct IBconn *conn;
{
	conn->db = 0;
	db_num--;
	if (ibconn_list == 0 || ibconn_list == conn)
		ibconn_list = 0;
	else {
		struct IBconn *list = ibconn_list;

		while (list->next) {
			if (list->next == conn) {
				list->next = conn->next;
				break;
			}
			list = list->next;
		}
	}
}

static void
conn_disconnect(conn)
	struct IBconn *conn;
{
	if (transact) {
		isc_commit_transaction(isc_status, &transact);
		ib_error_check();
	}
	isc_detach_database(isc_status, &conn->db);
	ib_error_check();
	conn_remove(conn);
}

static void
conn_disconnect_warn(conn)
	struct IBconn *conn;
{
	if (transact) {
		isc_commit_transaction(isc_status, &transact);
		ib_error_check_warn();
	}
	isc_detach_database(isc_status, &conn->db);
	ib_error_check_warn();
	conn_remove(conn);
}

static void
conn_mark(conn)
	struct IBconn *conn;
{
	rb_gc_mark(conn->curs);
}

static void
conn_free(conn)
	struct IBconn *conn;
{
	if (conn->db) {
		conn_disconnect_warn(conn);
	}
	free(conn);
}

static struct IBconn*
conn_check_retrieve(data)
	VALUE data;
{
	if (TYPE(data) != T_DATA || RDATA(data)->dfree != (void *)conn_free) {
		rb_raise(rb_eTypeError, "wrong argument type %s (expected InterBase::Connection)",
			rb_class2name(CLASS_OF(data)));
	}
	return (struct IBconn*)RDATA(data)->data;
}

static unsigned short
conn_db_SQL_Dialect(conn)
	struct IBconn *conn;
{
	long dialect;
	long length;
	char db_info_command = isc_info_db_SQL_dialect;

	/* Get the db SQL Dialect */
	isc_database_info(isc_status, &conn->db,
			1, &db_info_command,
			sizeof(isc_info_buff), isc_info_buff);
	ib_error_check();

	if (isc_info_buff[0] == isc_info_db_SQL_dialect) {
		length = isc_vax_integer(&isc_info_buff[1], 2);
		dialect = isc_vax_integer(&isc_info_buff[3], (short)length);
	}
	else
		dialect = 1;

	return dialect;
}

static unsigned short
conn_dialect(conn)
	struct IBconn *conn;
{
	return conn->dialect;
}

static unsigned short
conn_db_dialect(conn)
	struct IBconn *conn;
{
	return conn->db_dialect;
}

/* Transaction option list */

static trans_opts	rcom_opt_S[] =
{
	"NO",			"RECORD_VERSION",	isc_tpb_no_rec_version,	-1,	0,
	"RECORD_VERSION",	0,			isc_tpb_rec_version,	-1,	0,
	"*",			0,			isc_tpb_no_rec_version,	-1,	0,
	0,			0,			0,			0,	0
};


static trans_opts	read_opt_S[] =
{
	"WRITE",	0,	isc_tpb_write,		1,	0,
	"ONLY",		0,	isc_tpb_read,		1,	0,
	"COMMITTED",	0,	isc_tpb_read_committed,	2,	rcom_opt_S,
	0,		0,	0,			0,	0
};


static trans_opts	snap_opt_S[] =
{
	"TABLE",	"STABILITY",	isc_tpb_consistency,	2,	0,
	"*",		0,		isc_tpb_concurrency,	2,	0,
	0,			0,	0,			0,	0
};


static trans_opts	isol_opt_S[] =
{
	"SNAPSHOT",	0,		0,			0,	snap_opt_S,
	"READ",		"COMMITTED",	isc_tpb_read_committed,	2,	rcom_opt_S,
	0,		0,		0,			0,	0
};


static trans_opts	trans_opt_S[] =
{
	"READ",		0,		0,		0,	read_opt_S,
	"WAIT",		0,		isc_tpb_wait,	3,	0,
	"NO",		"WAIT",		isc_tpb_nowait,	3,	0,
	"ISOLATION",	"LEVEL",	0,		0,	isol_opt_S,
	"SNAPSHOT",	0,		0,		0,	snap_opt_S,
	"RESERVING",	0,		-1,		0,	0,
	0,		0,		0,		0,	0
};

/* Name1	Name2		Option value	    Position	Sub-option */

#define	RESV_TABLEEND	"FOR"
#define	RESV_SHARED	"SHARED"
#define	RESV_PROTECTD	"PROTECTED"
#define	RESV_READ	"READ"
#define	RESV_WRITE	"WRITE"
#define	RESV_CONTINUE	','

static char*
trans_parseopts(opt, tpb_len)
	VALUE *opt;
	int *tpb_len;
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
	char *tblend_p;
	int tbl_len;
	int res_first;
	int res_count;
	int ofs;
	char sp_prm;
	char rw_prm;
	int cont_f;
	char *desc = 0;

	/* Initialize */
	s = STR2CSTR(opt);
	trans = ALLOCA_N(char, strlen(s)+1);
	strcpy(trans, s);
	s = trans;
	while (*s) {
		*s = UPPER(*s);
		s++;
	}

	used = 0;
	size = 0;
	tpb = 0;
	memset((void *)check_f, 0, sizeof(check_f));

	/* Set the default transaction option */
	tpb = (char*)xmalloc(TPBBUFF_ALLOC);
	size = TPBBUFF_ALLOC;
	memcpy((void*)tpb, (void*)isc_tpb_0, sizeof(isc_tpb_0));
	used = sizeof(isc_tpb_0);

	/* Analize the transaction option strings */
	curr_p = trans_opt_S;
	check1_p = strtok(trans, CMND_DELIMIT);
	if (check1_p)
		check2_p = strtok(0, CMND_DELIMIT);
	else
		check2_p = 0;
	while (curr_p) {
		target_p = 0;
		next_c = 0;
		for (count = 0; curr_p[count].option1; count++) {
			if (!strcmp(curr_p[count].option1, "*")) {
				target_p = &curr_p[count];
				break;
			}
			else if (check1_p && !strcmp(check1_p, curr_p[count].option1)) {
				if (!curr_p[count].option2) {
					next_c = 1;
					target_p = &curr_p[count];
					break;
				}
				else if (check2_p && !strcmp(check2_p, curr_p[count].option2)) {
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
			}
			else {
				if (used + 1 > size) {
					tpb = (char *)realloc(tpb, size + TPBBUFF_ALLOC);
					size += TPBBUFF_ALLOC;
				}
				tpb[used] = target_p->optval;
				used++;
			}
		}
		else if (target_p->optval) {		/* RESERVING ... FOR */
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
					if (!strcmp(resend_p, RESV_TABLEEND))
						break;
					resend_p = strtok(0, CMND_DELIMIT);
				}

				if (!resend_p) {
					desc = "Illegal transaction option was specified";
					goto error;
				}

				while (resv_p < resend_p) {
					if (*resv_p == '\0' || (ofs = strspn(resv_p, LIST_DELIMIT)) < 0)
						resv_p++;
					else {
						resv_p = &resv_p[ofs];
						tblend_p = strpbrk(resv_p, LIST_DELIMIT);
						if (tblend_p)
							tbl_len = tblend_p - resv_p;
						else
							tbl_len = strlen(resv_p);
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
				if (resv_p && !strcmp(resv_p, RESV_SHARED))
					sp_prm = isc_tpb_shared;
				else if (resv_p && !strcmp(resv_p, RESV_PROTECTD))
					sp_prm = isc_tpb_protected;
				else {
					desc = "RESERVING needs {SHARED|PROTECTED} {READ|WRITE}";
					goto error;
				}

				cont_f = 0;
				resv_p = strtok(0, CMND_DELIMIT);
				if (resv_p) {
					if (resv_p[strlen(resv_p)-1] == RESV_CONTINUE) {
						cont_f = 1;
						resv_p[strlen(resv_p)-1] = '\0';
					}
					else {
						tblend_p = strpbrk(resv_p, LIST_DELIMIT);
						if (tblend_p) {
							cont_f = 2;
							*tblend_p = '\0';
						}
					}
				}

				if (resv_p && !strcmp(resv_p, RESV_READ))
					rw_prm = isc_tpb_lock_read;
				else if (resv_p && !strcmp(resv_p, RESV_WRITE))
					rw_prm = isc_tpb_lock_write;
				else {
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
				}
				else {
					resv_p = strtok(0, CMND_DELIMIT);
					if (resv_p) {
						if ((int)strlen(resv_p) == 1 && resv_p[0] == RESV_CONTINUE) {
							resv_p = strtok(0, CMND_DELIMIT);
							if (!resv_p) {
								desc = "Unexpected end of command";
								goto error;
							}
						}
						else if (resv_p[0] == RESV_CONTINUE)
							resv_p++;
						else {
							next_c = 1;
							check2_p = resv_p;
							resv_p = 0;
						}
					}
					else {
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
			if (check2_p)
				check2_p = strtok(0, CMND_DELIMIT);
		}

		if (check1_p && !curr_p)
			curr_p = trans_opt_S;
	}

	/* Set the results */
	*tpb_len = used;
	return tpb;

error:
	free(tpb);
	rb_raise(rb_eFbError, desc);
}

static void
set_teb_vec(vec, conn, tpb, len)
	ISC_TEB *vec;
	struct IBconn *conn;
	char *tpb;
	int len;
{
	vec->dbb_ptr = &conn->db;
	if (tpb) {
		vec->tpb_ptr = tpb;
		vec->tpb_len = len;
	}
	else {
		vec->tpb_ptr = 0;
		vec->tpb_len = 0;
	}
}

static void
trans_start(opt, argc, argv)
	VALUE opt;
	int argc;
	VALUE *argv;
{
	struct IBconn *conn;
	ISC_TEB *teb_vec = ALLOCA_N(ISC_TEB, db_num);
	ISC_TEB *vec = teb_vec;
	char *tpb = 0;
	short n;
	int tpb_len;

	if (transact) {
		rb_raise(rb_eFbError, "The transaction has been already started");
	}

	if (!NIL_P(opt)) {
		tpb = trans_parseopts(opt, &tpb_len);
	}

	if (argc > db_num) {
		rb_raise(rb_eFbError, "Too many databases specified for the transaction");
	}
	if (argc == 0) {
		n = db_num;
		for (conn=ibconn_list; conn; conn=conn->next) {
			set_teb_vec(vec, conn, tpb, tpb_len);
			vec++;
		}
	}
	else {
		for (n=0; n<argc; n++) {
			conn = conn_check_retrieve(argv[n]);
			set_teb_vec(vec, conn, tpb, tpb_len);
			vec++;
		}
	}

	isc_start_multiple(isc_status, &transact, n, teb_vec);
	if (tpb) free(tpb);
	ib_error_check();
}

/* transaction method */
static VALUE
ib_transaction(argc, argv, self)
	int argc;
	VALUE *argv;
	VALUE self;
{
	VALUE opt = Qnil;

	if (argc > 0) {
		opt = *argv++; argc--;
	}
	trans_start(opt, argc, argv);

	return Qnil;
}

static VALUE
ib_commit()
{
	conn_close_cursors();
	if (transact) {
		isc_commit_transaction(isc_status, &transact);
		ib_error_check();
		transact = 0;
	}
	return Qnil;
}

static VALUE
ib_rollback()
{
	conn_close_cursors();
	if (transact) {
		isc_rollback_transaction(isc_status, &transact);
		ib_error_check();
		transact = 0;
	}
	return Qnil;
}

static VALUE
ib_connect(argc, argv)
	int argc;
	VALUE *argv;
{
	return ibconn_s_new(argc, argv, rb_cFbConnection);
}

/* connection methods */
static VALUE
ibconn_s_new(argc, argv, klass)
	int argc;
	VALUE *argv;
	VALUE klass;
{
	VALUE a, b, c, d, obj;
	char *db, *user, *pass, *charset;
	struct IBconn *conn;
	isc_db_handle handle = 0;		/* database handle */
	char *isc_p;
	short isc_l = 0;

	user = pass = charset = 0;
	rb_scan_args(argc, argv, "13", &a, &b, &c, &d);
	db = STR2CSTR(a);
	if (!NIL_P(b)) user = STR2CSTR(b);
	if (!NIL_P(c)) pass = STR2CSTR(c);
	if (!NIL_P(d)) charset = STR2CSTR(d);

	isc_p = (char*)0;
	isc_expand_dpb(&isc_p, &isc_l,
		isc_dpb_user_name, user,
		isc_dpb_password, pass,
		isc_dpb_lc_ctype, (char*)charset, 0);
	isc_attach_database(isc_status, 0, db, &handle, isc_l, isc_p);
	isc_free(isc_p);
	ib_error_check();

	obj = Data_Make_Struct(klass, struct IBconn, conn_mark, conn_free, conn);
	conn->db = handle;
	transact = 0;
	i_sqlda = sqlda_alloc(SQLDA_COLSINIT);
	o_sqlda = sqlda_alloc(SQLDA_COLSINIT);
	results = paramts = 0;
	ressize = prmsize = 0;
	conn->curs = rb_ary_new();
	db_num++;
	conn->next = ibconn_list;
	ibconn_list = conn;

	{
		unsigned short dialect = SQL_DIALECT_CURRENT;
		unsigned short db_dialect = conn_db_SQL_Dialect(conn);

		if (db_dialect < dialect) {
			dialect = db_dialect;
			/* TODO: downgrade warning */
		}

		conn->dialect = dialect;
		conn->db_dialect = db_dialect;
	}

	if (rb_block_given_p()) {
		return rb_ensure(rb_yield,obj,ibconn_close,obj);
	}
	else {
		return obj;
	}
}

static VALUE
ibconn_cursor(obj)
    VALUE obj;
{
	VALUE c;
	struct IBconn *conn;
	struct IBcurs *curs;

	Data_Get_Struct(obj, struct IBconn, conn);
	conn_check(conn);

	c = Data_Make_Struct(rb_cFbCursor, struct IBcurs, curs_mark, curs_free, curs);
	curs->conn = obj;
	curs->describe = Qnil;
	curs->open = Qfalse;
	curs->stmt = 0;
	isc_dsql_alloc_statement2(isc_status, &conn->db, &curs->stmt);
	ib_error_check();

	return c;
}

static VALUE
ibconn_execute(argc, argv, obj)
    int argc;
    VALUE *argv;
    VALUE obj;
{
	VALUE curs = ibconn_cursor(obj);
	VALUE val;

	val = ibcurs_execute(argc, argv, curs);
	if (NIL_P(val)) {
		if (rb_block_given_p()) {
			return rb_ensure(rb_yield,curs,ibcurs_close,curs);
   		}
		else {
			return curs;
   		}
	}
	return Qnil;
}

static VALUE
ibconn_close(self)
	VALUE self;
{
	struct IBconn *conn;

	Data_Get_Struct(self, struct IBconn, conn);
	conn_check(conn);
	conn_disconnect(conn);
	conn_drop_cursors(conn);

	return Qnil;
}

static VALUE
ibconn_dialect(self)
	VALUE self;
{
	struct IBconn *conn;

	Data_Get_Struct(self, struct IBconn, conn);
	conn_check(conn);
	
	return INT2FIX(conn->dialect);
}

static VALUE
ibconn_db_dialect(self)
	VALUE self;
{
	struct IBconn *conn;

	Data_Get_Struct(self, struct IBconn, conn);
	conn_check(conn);
	
	return INT2FIX(conn->db_dialect);
}

/* cursor utilities */
static void
curs_check(curs)
	struct IBcurs *curs;
{
	if (curs->stmt == 0) {
		rb_raise(rb_eFbError, "dropped db cursor");
	}
	if (!curs->open) {
		rb_raise(rb_eFbError, "closed db cursor");
	}
}

static void
curs_drop(curs)
	struct IBcurs *curs;
{
	if (curs->open) {
		isc_dsql_free_statement(isc_status, &curs->stmt, DSQL_close);
		ib_error_check();
	}
	isc_dsql_free_statement(isc_status, &curs->stmt, DSQL_drop);
	ib_error_check();
	curs->stmt = 0;
}

static void
curs_drop_warn(curs)
	struct IBcurs *curs;
{
	if (curs->open) {
		isc_dsql_free_statement(isc_status, &curs->stmt, DSQL_close);
		ib_error_check_warn();
	}
	isc_dsql_free_statement(isc_status, &curs->stmt, DSQL_drop);
	ib_error_check_warn();
	curs->stmt = 0;
}

static void
curs_mark(curs)
	struct IBcurs *curs;
{
	rb_gc_mark(curs->conn);
	rb_gc_mark(curs->describe);
}

static void
curs_free(curs)
	struct IBcurs *curs;
{
	if (curs->stmt)
		curs_drop_warn(curs);
	free(curs);
}

struct time_object {
    struct timeval tv;
    struct tm tm;
    int gmt;
    int tm_got;
};

#define GetTimeval(obj, tobj) \
    Data_Get_Struct(obj, struct time_object, tobj)

static void
set_inputparams(curs, argc, argv)
	struct IBcurs *curs;
	int argc;
	VALUE *argv;
{
	struct IBconn *conn;
	long count;
	long offset;
	long type;
	short dtp;
	VALUE obj;
	long lvalue;
	long alignment;
	double dvalue;
	double dcheck;
	VARY *vary;
	XSQLVAR *var;

	isc_blob_handle blob_handle;
	ISC_QUAD blob_id;
	static char blob_items[] = { isc_info_blob_max_segment };
	char blob_info[16];
	char *p;
	unsigned short length;
	struct time_object *tobj;

	Data_Get_Struct(curs->conn, struct IBconn, conn);

	/* Check the number of parameters */
	if (i_sqlda->sqld != argc) {
		rb_raise(rb_eFbError, "statement requires %d items; %d given",
			i_sqlda->sqld, argc);
	}

	/* Get the parameters */
	for (count = 0,offset = 0; count < argc; count++) {
		obj = argv[count];

		type = TYPE(obj);

		/* Convert the data type for InterBase */
		var = &i_sqlda->sqlvar[count];
		if (!NIL_P(obj)) {
			dtp = var->sqltype & ~1;		/* Erase null flag */
			alignment = var->sqllen;

			switch (dtp) {
				case SQL_TEXT :
					alignment = 1;
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					obj = rb_obj_as_string(obj);
					memcpy(var->sqldata, RSTRING(obj)->ptr, RSTRING(obj)->len);
					var->sqllen = RSTRING(obj)->len;
					offset += var->sqllen + 1;
					break;

				case SQL_VARYING :
					alignment = sizeof(short);
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					vary = (VARY *)var->sqldata;
					obj = rb_obj_as_string(obj);
					memcpy(vary->vary_string, RSTRING(obj)->ptr, RSTRING(obj)->len);
					vary->vary_length = RSTRING(obj)->len;
					offset += vary->vary_length + sizeof(short);
					break;

				case SQL_SHORT :
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					lvalue = NUM2LONG(obj);
					if (lvalue < SHRT_MIN || lvalue > SHRT_MAX) {
						rb_raise(rb_eIOError, "short integer overflow");
					}
					*(short *)var->sqldata = lvalue;
					offset += alignment;
					break;

				case SQL_LONG :
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					lvalue = NUM2LONG(obj);
					*(long *)var->sqldata = lvalue;
					offset += alignment;
					break;

				case SQL_FLOAT :
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					dvalue = NUM2DBL(obj);
					if (dvalue >= 0.0)
						dcheck = dvalue;
					else
						dcheck = dvalue * -1;

					if (dcheck != 0.0 && (dcheck < FLT_MIN || dcheck > FLT_MAX)) {
						rb_raise(rb_eIOError, "float overflow");
					}
					*(float *)var->sqldata = dvalue;
					offset += alignment;
					break;

				case SQL_DOUBLE :
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					dvalue = NUM2DBL(obj);
					*(double *)var->sqldata = dvalue;
					offset += alignment;
					break;
#if HAVE_LONG_LONG
				case SQL_INT64 :
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					*(ISC_INT64 *)var->sqldata = NUM2LL(obj);
					offset += alignment;
					break;
#endif
				case SQL_BLOB :
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					obj = rb_obj_as_string(obj);

					blob_handle = NULL;
					isc_create_blob2(
						isc_status,&conn->db,&transact,
						&blob_handle,&blob_id,0,NULL);
					ib_error_check();
					length = RSTRING(obj)->len;
					p = RSTRING(obj)->ptr;
					while (length >= 4096) {
						isc_put_segment(isc_status,&blob_handle,4096,p);
						ib_error_check();
						p += 4096;
						length -= 4096;
					}
					if (length) {
						isc_put_segment(isc_status,&blob_handle,length,p);
						ib_error_check();
					}
					isc_close_blob(isc_status,&blob_handle);
					ib_error_check();

					*(ISC_QUAD *)var->sqldata = blob_id;
					offset += alignment;
					break;

				case SQL_TIMESTAMP :
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					GetTimeval(obj, tobj);
					isc_encode_timestamp(&tobj->tm, (ISC_TIMESTAMP *)var->sqldata);
					offset += alignment;
					break;

				case SQL_TYPE_TIME :
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					GetTimeval(obj, tobj);
					isc_encode_sql_time(&tobj->tm, (ISC_TIME *)var->sqldata);
					offset += alignment;
					break;

				case SQL_TYPE_DATE :
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					GetTimeval(obj, tobj);
					isc_encode_sql_date(&tobj->tm, (ISC_DATE *)var->sqldata);
					offset += alignment;
					break;

#if 0
				case SQL_ARRAY :
					/* Not supported now
					offset = ALIGN(offset, alignment);
					var->sqldata = (char *)(paramts + offset);
					if (get_arrayvalue(self, type, obj, var))
						return(STATUS_ABNORMAL);
					offset += alignment;
					break;
					*/
					rb_raise(rb_eIOError, "Arrays not supported");
					break;
#endif

				default :
					rb_raise(rb_eFbError, "Specified table includes unsupported datatype (%d)", dtp);
			}

			if (var->sqltype & 1) {
				offset = ALIGN(offset, sizeof(short));
				var->sqlind = (short *)(paramts + offset);
				*var->sqlind = 0;
				offset += sizeof(short);
			}
		}
		else if (var->sqltype & 1) {
			var->sqldata = 0;
			offset = ALIGN(offset, sizeof(short));
			var->sqlind = (short *)(paramts + offset);
			*var->sqlind = -1;
			offset += sizeof(short);
		}
		else {
			rb_raise(rb_eFbError, "specified column is not permitted to be null");
		}
	}
}

static void
execute_withparams(curs, argc, argv)
	struct IBcurs *curs;
	int argc;
	VALUE *argv;
{
	struct IBconn *conn;

	Data_Get_Struct(curs->conn, struct IBconn, conn);
	/* Check the first object type of the parameters */
	if (argc >= 1 && TYPE(argv[0]) == T_ARRAY) {
		VALUE obj;
		int i;

		for (i=0; i<argc; i++) {
			obj = argv[i];

			/* Set the input parameters */
			Check_Type(obj, T_ARRAY);
			set_inputparams(curs, RARRAY(obj)->len, RARRAY(obj)->ptr);

			/* Execute SQL statement */
			isc_dsql_execute2(isc_status, &transact, &curs->stmt, 1, i_sqlda, 0);
			ib_error_check();
		}
	}
	else {
		/* Set the input parameters */
		set_inputparams(curs, argc, argv);

		/* Execute SQL statement */
		isc_dsql_execute2(isc_status, &transact, &curs->stmt, 1, i_sqlda, 0);
		ib_error_check();
	}
}

static VALUE
curs_description(sqlda)
	XSQLDA *sqlda;
{
	long cols;
	long count;
	VALUE ary;
	XSQLVAR *var;
	short dtp;

	/* Check the number of result columns */
	cols = sqlda->sqld;
	if (cols == 0) {
		return Qnil;
	}

	/* Create array */
	ary = rb_ary_new();

	/* Check the number of result columns */
	for (count = 0; count < cols; count++) {
		VALUE colary;
		VALUE str;

		colary = rb_ary_new();

		var = &sqlda->sqlvar[count];
		dtp = var->sqltype & ~1;

		/* columns name */
		str = rb_tainted_str_new(var->sqlname, var->sqlname_length);
		rb_str_freeze(str);
		rb_ary_push(colary, str);
		/* type code */
		rb_ary_push(colary, INT2NUM((long)(var->sqltype & ~1)));
		/* display size */
		rb_ary_push(colary, INT2NUM((long)var->sqllen));

		/* internal size */
		if (dtp == SQL_VARYING)
			rb_ary_push(colary, INT2NUM((long)var->sqllen + sizeof(short)));
		else
			rb_ary_push(colary, INT2NUM((long)var->sqllen));

		/* precision */
		rb_ary_push(colary, INT2FIX(0));

		/* scale */
		rb_ary_push(colary, INT2NUM((long)var->sqlscale));

		/* null OK */
		rb_ary_push(colary, (var->sqltype & 1)?Qtrue:Qfalse);

		rb_ary_freeze(colary);
		rb_ary_push(ary, colary);
	}
	rb_ary_freeze(ary);
	return ary;
}

/* Check the input parameters */
static void
curs_check_inparams(curs, argc, argv, exec)
	struct IBcurs *curs;
	int argc;
	VALUE *argv;
	int exec;
{
	long items;

	/* Check the parameter */
	if (argc == 0) {
		rb_raise(rb_eFbError, "input parameters must be specified");
	}

	/* Execute specified process */
	switch (exec) {
		case EXECF_EXECDML:
			execute_withparams(curs, argc, argv);
			break;
		case EXECF_SETPARM:
			set_inputparams(curs, argc, argv);
			break;
		default:
			rb_raise(rb_eFbError, "should specify either EXECF_EXECDML or EXECF_SETPARM");
			break;
	}
}

static void
curs_fetch_prep(curs)
	struct IBcurs *curs;
{
	struct IBconn *conn;
	long cols;
	long count;
	XSQLVAR *var;
	short dtp;
	long length;
	long alignment;
	long offset;

	curs_check(curs);

	Data_Get_Struct(curs->conn, struct IBconn, conn);
	conn_check(conn);

	/* Check if open cursor */
	if (!curs->open) {
		rb_raise(rb_eFbError, "The cursor has not been open; use execute(query)");
	}
	/* Describe output SQLDA */
	isc_dsql_describe(isc_status, &curs->stmt, 1, o_sqlda);
	ib_error_check();

	/* Set the output SQLDA */
	cols = o_sqlda->sqld;
	for (var=o_sqlda->sqlvar,offset=0,count=0; count<cols; var++,count++) {
		length = alignment = var->sqllen;
		dtp = var->sqltype & ~1;

		if (dtp == SQL_TEXT)
			alignment = 1;
		else if (dtp == SQL_VARYING) {
			length += sizeof(short);
			alignment = sizeof(short);
		}
		offset = ALIGN(offset, alignment);
		var->sqldata = (char*)(results + offset);
		offset += length;
		offset = ALIGN(offset, sizeof(short));
		var->sqlind = (short*)(results + offset);
		offset += sizeof(short);
	}
}

static VALUE
curs_fetch(curs)
	struct IBcurs *curs;
{
	struct IBconn *conn;
	long cols;
	VALUE ary;
	long count;
	XSQLVAR *var;
	long dtp;
	VALUE val;
	VARY *vary;
	double ratio;
	double dval;
	long scnt;
	struct tm tms;

	isc_blob_handle blob_handle;
	ISC_QUAD blob_id;
	unsigned short actual_seg_len;
	time_t t;
	static char blob_items[] = {
		isc_info_blob_max_segment,
		isc_info_blob_num_segments,
		isc_info_blob_total_length
	};
	char blob_info[32];
	char *p, item;
	short length;
	unsigned short max_segment;
	ISC_LONG num_segments;
	ISC_LONG total_length;

	Data_Get_Struct(curs->conn, struct IBconn, conn);
	conn_check(conn);

	/* Fetch one row */
	if (isc_dsql_fetch(isc_status, &curs->stmt, 1, o_sqlda) == SQLCODE_NOMORE) {
		return Qnil;
	}
	ib_error_check();

	/* Create the result tuple object */
	cols = o_sqlda->sqld;
	ary = rb_ary_new2(cols);

	/* Create the result objects for each columns */
	for (count = 0; count < cols; count++) {
		var = &o_sqlda->sqlvar[count];
		dtp = var->sqltype & ~1;

		/* Check if column is null */

		if ((var->sqltype & 1) && (*var->sqlind < 0)) {
			val = Qnil;
		}
		else {
			/* Set the column value to the result tuple */

			switch (dtp) {
				case SQL_TEXT:
					val = rb_tainted_str_new(var->sqldata, var->sqllen);
					break;

				case SQL_VARYING:
					vary = (VARY*)var->sqldata;
					val = rb_tainted_str_new(vary->vary_string, vary->vary_length);
					break;

				case SQL_SHORT:
					if (var->sqlscale < 0) {
						ratio = 1;
						for (scnt = 0; scnt > var->sqlscale; scnt--)
							ratio *= 10;
						dval = (double)*(short*)var->sqldata/ratio;
						val = rb_float_new(dval);
					}
					else {
						val = INT2NUM((long)*(short*)var->sqldata);
					}
					break;

				case SQL_LONG:
					if (var->sqlscale < 0) {
						ratio = 1;
						for (scnt = 0; scnt > var->sqlscale; scnt--)
						ratio *= 10;
						dval = (double)*(long*)var->sqldata/ratio;
						val = rb_float_new(dval);
					}
					else {
						val = INT2NUM(*(long*)var->sqldata);
					}
					break;

				case SQL_FLOAT:
					val = rb_float_new((double)*(float*)var->sqldata);
					break;

				case SQL_DOUBLE:
					val = rb_float_new(*(double*)var->sqldata);
					break;
#if HAVE_LONG_LONG
				case SQL_INT64:
					val = LL2NUM(*(LONG_LONG*)var->sqldata);
					break;
#endif
				case SQL_TIMESTAMP:
					isc_decode_timestamp((ISC_TIMESTAMP *)var->sqldata, &tms);
					t = mktime(&tms);
					if (t < 0) t = 0;
					val = rb_time_new(t, 0);
					break;

				case SQL_TYPE_TIME:
					isc_decode_sql_time((ISC_TIME *)var->sqldata, &tms);
					t = mktime(&tms);
					if (t < 0) t = 0;
					val = rb_time_new(t, 0);
					break;

				case SQL_TYPE_DATE:
					isc_decode_sql_date((ISC_DATE *)var->sqldata, &tms);
					t = mktime(&tms);
					if (t < 0) t = 0;
					val = rb_time_new(t, 0);
					break;

				case SQL_BLOB:
					blob_handle = NULL;
					blob_id = *(ISC_QUAD *)var->sqldata;
					isc_open_blob2(isc_status,&conn->db,&transact,&blob_handle,&blob_id,0,NULL);
					ib_error_check();
					isc_blob_info(
						isc_status, &blob_handle,
						sizeof(blob_items), blob_items,
						sizeof(blob_info), blob_info);
					ib_error_check();
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
					for (p = RSTRING(val)->ptr; num_segments > 0; num_segments--, p += actual_seg_len) {
						isc_get_segment(isc_status,&blob_handle,&actual_seg_len,max_segment,p);
						ib_error_check();
					}
					isc_close_blob(isc_status,&blob_handle);
					ib_error_check();
					break;

				case SQL_ARRAY:
					rb_warn("ARRAY not supported (yet)");
					val = Qnil;
					break;

				default:
					rb_raise(rb_eFbError, "Specified table includes unsupported datatype (%d)", dtp);
					break;
			}
		}
		rb_ary_push(ary, val);
	}

	return ary;
}

/* cursor methods */
static VALUE
ibcurs_execute(argc, argv, self)
	int argc;
	VALUE *argv;
	VALUE self;
{
	struct IBcurs *curs;
	struct IBconn *conn;
	char *sql;
	long statement;
	long length;
	long in_params;
	long cols;
	long items;

	Data_Get_Struct(self, struct IBcurs, curs);

	Data_Get_Struct(curs->conn, struct IBconn, conn);
	conn_check(conn);

	if (argc < 1) {
		rb_raise(rb_eArgError, "too few arguments (at least 1)");
	}
	sql = STR2CSTR(*argv);
	argc--; argv++;

	if (curs->open) {
		isc_dsql_free_statement(isc_status, &curs->stmt, DSQL_close);
		ib_error_check();
		curs->open = Qfalse;
	}
	if (!transact) trans_start(Qnil, 0, 0);

	/* Prepare query */
	isc_dsql_prepare(isc_status, &transact, &curs->stmt, 0, sql, conn_dialect(conn), o_sqlda);
	ib_error_check();

	/* Get the statement type */
	isc_dsql_sql_info(isc_status, &curs->stmt,
			sizeof(isc_info_stmt), isc_info_stmt,
			sizeof(isc_info_buff), isc_info_buff);
	ib_error_check();

	if (isc_info_buff[0] == isc_info_sql_stmt_type) {
		length = isc_vax_integer(&isc_info_buff[1], 2);
		statement = isc_vax_integer(&isc_info_buff[3], (short)length);
	}
	else
		statement = 0;

	/* Describe the parameters */
	isc_dsql_describe_bind(isc_status, &curs->stmt, 1, i_sqlda);
	ib_error_check();

	isc_dsql_describe(isc_status, &curs->stmt, 1, o_sqlda);
	ib_error_check();

	/* Get the number of parameters and reallocate the SQLDA */
	in_params = i_sqlda->sqld;
	if (i_sqlda->sqln < in_params) {
		free(i_sqlda);
		i_sqlda = sqlda_alloc(in_params);
		/* Describe again */
		isc_dsql_describe_bind(isc_status, &curs->stmt, 1, i_sqlda);
		ib_error_check();
	}

    /* Get the size of parameters buffer and reallocate it */
	if (in_params) {
		length = calculate_buffsize(i_sqlda);
		if (length > prmsize) {
			paramts = xrealloc(paramts, length);
			prmsize = length;
		}
	}

    /* Execute the SQL statement if it is not query */
	if (!o_sqlda->sqld) {
		if (statement == isc_info_sql_stmt_start_trans) {
			rb_raise(rb_eFbError, "use InterBase::Connection#transaction()");
		}
		else if (statement == isc_info_sql_stmt_commit) {
			rb_raise(rb_eFbError, "use InterBase::Connection#commit()");
		}
		else if (statement == isc_info_sql_stmt_rollback) {
			rb_raise(rb_eFbError, "use InterBase::Connection#rollback()");
		}
		else if (in_params) {
			curs_check_inparams(curs, argc, argv, EXECF_EXECDML);
		}
		else {
			isc_dsql_execute2(isc_status, &transact, &curs->stmt, 1, 0, 0);
			ib_error_check();
		}
	}
	/* Open cursor if the SQL statement is query */
	else {
		/* Get the number of columns and reallocate the SQLDA */
		cols = o_sqlda->sqld;
		if (o_sqlda->sqln < cols) {
			free(o_sqlda);
			o_sqlda = sqlda_alloc(cols);
			/* Describe again */
			isc_dsql_describe(isc_status, &curs->stmt, 1, o_sqlda);
			ib_error_check();
		}

		if (in_params) {
			curs_check_inparams(curs, argc, argv, EXECF_SETPARM);
		}

#if 0
		/* Declare cursor */
		isc_dsql_set_cursor_name(isc_status, &curs->stmt, self->curs_name, 0);
		ib_error_check();
#endif

		/* Open cursor */
		isc_dsql_execute2(isc_status, &transact,
				&curs->stmt, 1,
				in_params?i_sqlda:0, 0);
		ib_error_check();
		curs->open = Qtrue;

		/* Get the size of results buffer and reallocate it */
		length = calculate_buffsize(o_sqlda);
		if (length > ressize) {
			results = xrealloc(results, length);
			ressize = length;
		}

		/* Set the description attributes */
		curs->describe = curs_description(o_sqlda);
	}
	/* Set the return object */
	if (statement == isc_info_sql_stmt_select ||
		statement == isc_info_sql_stmt_select_for_upd) {
		return Qnil;
	}
	else if (statement == isc_info_sql_stmt_ddl) {
		return INT2NUM(STATEMENT_DDL);
	}
	return INT2NUM(STATEMENT_DML);
}

static VALUE
ibcurs_fetch(self)
	VALUE self;
{
	struct IBcurs *curs;

	Data_Get_Struct(self, struct IBcurs, curs);
	curs_fetch_prep(curs);

	return curs_fetch(curs);
}

static VALUE
ibcurs_fetchall(self)
	VALUE self;
{
	VALUE ary, row;
	struct IBcurs *curs;

	Data_Get_Struct(self, struct IBcurs, curs);
	curs_fetch_prep(curs);

	ary = rb_ary_new();
	for (;;) {
		row = curs_fetch(curs);
		if (NIL_P(row)) break;
		rb_ary_push(ary, row);
	}

	return ary;
}

static VALUE
ibcurs_each(self)
	VALUE self;
{
	VALUE ary, row;
	struct IBcurs *curs;

	Data_Get_Struct(self, struct IBcurs, curs);
	curs_fetch_prep(curs);

	for (;;) {
		row = curs_fetch(curs);
		if (NIL_P(row)) break;
		rb_yield(row);
	}

	return Qnil;
}

static VALUE
ibcurs_close(self)
	VALUE self;
{
	struct IBcurs *curs;

	Data_Get_Struct(self, struct IBcurs, curs);
	curs_check(curs);

	/* Close the cursor */
	if (curs->stmt) {
		isc_dsql_free_statement(isc_status, &curs->stmt, DSQL_close);
		ib_error_check();
		curs->open = Qfalse;
	}
	curs->describe = Qnil;

	return Qnil;
}


static VALUE
ibcurs_drop(self)
	VALUE self;
{
	struct IBcurs *curs;
	struct IBconn *conn;
	int i;

	Data_Get_Struct(self, struct IBcurs, curs);
	curs_drop(curs);
	curs->describe = Qnil;

	/* reset the reference from connection */
	Data_Get_Struct(curs->conn, struct IBconn, conn);
	for (i=0; i<RARRAY(conn->curs)->len; i++) {
		if (RARRAY(conn->curs)->ptr[i] == self) {
			RARRAY(conn->curs)->ptr[i] = Qnil;
		}
	}

	return Qnil;
}

static VALUE
ibcurs_description(self)
	VALUE self;
{
	struct IBcurs *curs;

	Data_Get_Struct(self, struct IBcurs, curs);
	return curs->describe;
}

static VALUE
iberr_err_code(err)
	VALUE err;
{
	rb_p(err);
	return rb_iv_get(err, "error_code");
}

void
Init_fb()
{
	rb_mFb = rb_define_module("Fb");
	rb_define_module_function(rb_mFb, "connect", ib_connect, -1);
	rb_define_module_function(rb_mFb, "transaction", ib_transaction, -1);
	rb_define_module_function(rb_mFb, "commit", ib_commit, 0);
	rb_define_module_function(rb_mFb, "rollback", ib_rollback, 0);

	rb_cFbConnection = rb_define_class_under(rb_mFb, "Connection", rb_cData);
	rb_define_singleton_method(rb_cFbConnection, "new", ibconn_s_new, -1);
	rb_define_singleton_method(rb_cFbConnection, "open", ibconn_s_new, -1);
	rb_define_singleton_method(rb_cFbConnection, "connect", ibconn_s_new, -1);
	rb_define_method(rb_cFbConnection, "cursor", ibconn_cursor, 0);
	rb_define_method(rb_cFbConnection, "execute", ibconn_execute, -1);
	rb_define_method(rb_cFbConnection, "transaction", ib_transaction, -1);
	rb_define_method(rb_cFbConnection, "commit", ib_commit, 0);
	rb_define_method(rb_cFbConnection, "rollback", ib_rollback, 0);
	rb_define_method(rb_cFbConnection, "close", ibconn_close, 0);
	rb_define_method(rb_cFbConnection, "dialect", ibconn_dialect, 0);
	rb_define_method(rb_cFbConnection, "db_dialect", ibconn_db_dialect, 0);

	rb_cFbCursor = rb_define_class_under(rb_mFb, "Cursor", rb_cData);
	rb_define_method(rb_cFbCursor, "execute", ibcurs_execute, -1);
	rb_define_method(rb_cFbCursor, "description", ibcurs_description, 0);
	rb_define_method(rb_cFbCursor, "fetch", ibcurs_fetch, 0);
	rb_define_method(rb_cFbCursor, "fetchall", ibcurs_fetchall, 0);
	rb_define_method(rb_cFbCursor, "each", ibcurs_each, 0);
	rb_define_method(rb_cFbCursor, "close", ibcurs_close, 0);
	rb_define_method(rb_cFbCursor, "drop", ibcurs_drop, 0);

	rb_cFbBlob = rb_define_class_under(rb_mFb, "Blob", rb_cData);

	rb_eFbError = rb_define_class_under(rb_mFb, "Error", rb_eStandardError);
	rb_define_method(rb_eFbError, "error_code", iberr_err_code, 0);
}
