/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008-2015 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2015 Los Alamos National Security, LLC.
 *                         All rights reserved.
 * Copyright (c) 2013-2017 Intel, Inc. All rights reserved.
 * Copyright (c) 2016-2017 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      Amazon.com, Inc. or its affiliates.
 *                         All Rights reserved.
 * Copyright (c) 2018      Triad National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

/** @file **/

#include "opal_config.h"

#include "opal/class/opal_object.h"
#include "opal/util/output.h"
#include "opal/util/malloc.h"
#include "opal/util/proc.h"
#include "opal/memoryhooks/memory.h"
#include "opal/runtime/opal.h"
#include "opal/constants.h"
#include "opal/frameworks.h"
#include "opal/threads/tsd.h"
#include "opal/runtime/opal_cr.h"
#include "opal/runtime/opal_progress.h"

extern int opal_initialized;
extern int opal_util_initialized;
extern bool opal_init_called;

static opal_mutex_t opal_finalize_cleanup_fns_lock = OPAL_MUTEX_STATIC_INIT;
opal_list_t opal_finalize_cleanup_fns = {{0}};

struct opal_cleanup_fn_item_t {
    opal_list_item_t super;
    opal_cleanup_fn_t cleanup_fn;
#if OPAL_ENABLE_DEBUG
    char *cleanup_fn_name;
#endif
};

typedef struct opal_cleanup_fn_item_t opal_cleanup_fn_item_t;
OBJ_CLASS_DECLARATION(opal_cleanup_fn_item_t);

static void opal_cleanup_fn_item_construct (opal_cleanup_fn_item_t *item)
{
#if OPAL_ENABLE_DEBUG
    item->cleanup_fn_name = NULL;
#endif
}

static void opal_cleanup_fn_item_destruct (opal_cleanup_fn_item_t *item)
{
#if OPAL_ENABLE_DEBUG
    free (item->cleanup_fn_name);
    item->cleanup_fn_name = NULL;
#endif
}


OBJ_CLASS_INSTANCE(opal_cleanup_fn_item_t, opal_list_item_t,
                   opal_cleanup_fn_item_construct, opal_cleanup_fn_item_destruct);

void opal_finalize_append_cleanup (opal_cleanup_fn_t cleanup_fn, const char *fn_name)
{
    opal_cleanup_fn_item_t *cleanup_item = OBJ_NEW(opal_cleanup_fn_item_t);
    assert (NULL != cleanup_item);
    cleanup_item->cleanup_fn = cleanup_fn;
#if OPAL_ENABLE_DEBUG
    cleanup_item->cleanup_fn_name = strdup (fn_name);
    assert (NULL != cleanup_item->cleanup_fn_name);
#else
    (void) fn_name;
#endif

    opal_mutex_lock (&opal_finalize_cleanup_fns_lock);
    opal_list_append (&opal_finalize_cleanup_fns, &cleanup_item->super);
    opal_mutex_unlock (&opal_finalize_cleanup_fns_lock);
}

int opal_finalize_util (void)
{
    opal_cleanup_fn_item_t *cleanup_item;

    if( --opal_util_initialized != 0 ) {
        if( opal_util_initialized < 0 ) {
            return OPAL_ERROR;
        }
        return OPAL_SUCCESS;
    }

    /* call any registered cleanup functions before tearing down OPAL */
    OPAL_LIST_FOREACH_REV(cleanup_item, &opal_finalize_cleanup_fns, opal_cleanup_fn_item_t) {
        cleanup_item->cleanup_fn ();
    }

    OPAL_LIST_DESTRUCT(&opal_finalize_cleanup_fns);

    /* close interfaces code. */
    (void) mca_base_framework_close(&opal_if_base_framework);
    (void) mca_base_framework_close(&opal_installdirs_base_framework);

    /* finalize the class/object system */
    opal_class_finalize();

    free (opal_process_info.nodename);
    opal_process_info.nodename = NULL;

    return OPAL_SUCCESS;
}


int
opal_finalize(void)
{
    if( --opal_initialized != 0 ) {
        if( opal_initialized < 0 ) {
            return OPAL_ERROR;
        }
        return OPAL_SUCCESS;
    }

    opal_progress_finalize();

    /* close the checkpoint and restart service */
    opal_cr_finalize();

#if OPAL_ENABLE_FT_CR    == 1
    (void) mca_base_framework_close(&opal_compress_base_framework);
#endif

    (void) mca_base_framework_close(&opal_reachable_base_framework);

    (void) mca_base_framework_close(&opal_event_base_framework);

    /* close high resolution timers */
    (void) mca_base_framework_close(&opal_timer_base_framework);

    (void) mca_base_framework_close(&opal_backtrace_base_framework);
    (void) mca_base_framework_close(&opal_memchecker_base_framework);

    /* close the memcpy framework */
    (void) mca_base_framework_close(&opal_memcpy_base_framework);

    /* finalize the memory manager / tracker */
    opal_mem_hooks_finalize();

    /* close the hwloc framework */
    (void) mca_base_framework_close(&opal_hwloc_base_framework);

    /* close the shmem framework */
    (void) mca_base_framework_close(&opal_shmem_base_framework);

    /* cleanup the main thread specific stuff */
    opal_tsd_keys_destruct();

    /* finalize util code */
    opal_finalize_util();

    return OPAL_SUCCESS;
}
