/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2014-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal_config.h"
#include <assert.h>

#include "support.h"
#include "opal/class/opal_interval_tree.h"
#include "opal/runtime/opal.h"
#include "opal/constants.h"

#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <time.h>
#include <inttypes.h>
#include <assert.h>

#include <sys/time.h>

#if 0
static void dump (uint64_t low, uint64_t high, const void *data)
{
    fprintf (stderr, "Tree item: %16p, low: 0x%016" PRIx64 ", 0x%016" PRIx64 "\n", data, low, high);
}
#endif

int main (int argc, char *argv[]) {
    opal_interval_tree_t interval_tree;
    const unsigned int base_count = 2048;
    struct timespec start, stop;
    uintptr_t bases[base_count];
    unsigned long total;
    int rc;

    rc = opal_init_util (&argc, &argv);
    test_verify_int(OPAL_SUCCESS, rc);
    if (OPAL_SUCCESS != rc) {
        test_finalize();
        exit (1);
    }

    test_init("opal_interval_tree_t");

    OBJ_CONSTRUCT(&interval_tree, opal_interval_tree_t);

    rc = opal_interval_tree_init (&interval_tree);
    test_verify_int(OPAL_SUCCESS, rc);
    if (OPAL_SUCCESS != rc) {
        test_finalize();
        exit (1);
    }

    srandom (time(NULL));

    for (unsigned int i = 0 ; i < base_count ; ++i) {
        bases[i] = random () & 0x0007fffffffff000ul;
    }

    clock_gettime (CLOCK_MONOTONIC, &start);

    for (unsigned int i = 0 ; i < base_count ; ++i) {
        opal_interval_tree_insert (&interval_tree, (void *) (intptr_t) (i + 1), bases[i], bases[i] + 16384);
    }

    clock_gettime (CLOCK_MONOTONIC, &stop);

    total = (stop.tv_sec - start.tv_sec) * 1000000000 + stop.tv_nsec - start.tv_nsec;

    fprintf (stderr, "Average time to insert a new interval: %lu ns\n", total / base_count);

    fprintf (stderr, "Tree depth: %lu\n", (unsigned long) opal_interval_tree_depth (&interval_tree));

    clock_gettime (CLOCK_MONOTONIC, &start);

    for (unsigned int i = 0 ; i < base_count ; ++i) {
        void *ret = opal_interval_tree_find_overlapping (&interval_tree, bases[i], bases[i] + 16384);
        assert (NULL != ret);
    }

    clock_gettime (CLOCK_MONOTONIC, &stop);

    total = (stop.tv_sec - start.tv_sec) * 1000000000 + stop.tv_nsec - start.tv_nsec;

    fprintf (stderr, "Average time to find an existing interval: %lu ns\n", total / base_count);

    /* opal_interval_tree_traverse (&interval_tree, 0, 0x00007ffffffff000ul, false, dump); */

    OBJ_DESTRUCT(&interval_tree);

    opal_finalize_util ();

    return test_finalize ();
}
