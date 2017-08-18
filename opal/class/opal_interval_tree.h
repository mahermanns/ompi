/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2015-2017 Los Alamos National Security, LLC. All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */

/** @file
 *
 *     A red black tree
 */

#ifndef OPAL_INTERVAL_TREE_H
#define OPAL_INTERVAL_TREE_H

#include "opal_config.h"
#include <stdlib.h>
#include "opal/constants.h"
#include "opal/class/opal_object.h"
#include "opal/class/opal_free_list.h"

BEGIN_C_DECLS
/*
 * Data structures and datatypes
 */

/**
  * red and black enum
  */
typedef enum {RED, BLACK} opal_interval_tree_nodecolor_t;

/**
  * node data structure
  */
struct opal_interval_tree_node_t
{
    opal_free_list_item_t super;        /**< the parent class */
    opal_interval_tree_nodecolor_t color;     /**< the node color */
    struct opal_interval_tree_node_t *parent;/**< the parent node, can be NULL */
    struct opal_interval_tree_node_t *left;  /**< the left child - can be nill */
    struct opal_interval_tree_node_t *right; /**< the right child - can be nill */
    volatile int32_t ref_count;
    volatile int32_t lock;
    /** data for this interval */
    void *data;
    /** low value of this interval */
    uint64_t low;
    /** high value of this interval */
    uint64_t high;
    /** maximum value of all intervals in tree rooted at this node */
    uint64_t max;
};
typedef struct opal_interval_tree_node_t opal_interval_tree_node_t;

/**
  * the data structure that holds all the needed information about the tree.
  */
struct opal_interval_tree_t {
    opal_object_t super;           /**< the parent class */
    /* this root pointer doesn't actually point to the root of the tree.
     * rather, it points to a sentinal node who's left branch is the real
     * root of the tree. This is done to eliminate special cases */
    opal_interval_tree_node_t root; /**< a pointer to the root of the tree */
    opal_interval_tree_node_t nill;     /**< the nill sentinal node */
    opal_free_list_t free_list;   /**< the free list to get the memory from */
    volatile size_t tree_size;                  /**< the size of the tree */
};
typedef struct opal_interval_tree_t opal_interval_tree_t;

/** declare the tree node as a class */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_interval_tree_node_t);
/** declare the tree as a class */
OPAL_DECLSPEC OBJ_CLASS_DECLARATION(opal_interval_tree_t);

/* Function pointers for map traversal function */
/**
  * this function is used for the opal_interval_tree_traverse function.
  * it is passed a pointer to the value for each node and, if it returns
  * a one, the action function is called on that node. Otherwise, the node is ignored.
  */
typedef int (*opal_interval_tree_condition_fn_t)(void *);
/**
 * this function is used for the user to perform any action on the passed
 * values. The first argument is the key and the second is the value.
 * note that this function SHOULD NOT modify the keys, as that would
 * mess up the tree.
 */
typedef void (*opal_interval_tree_action_fn_t)(uint64_t low, uint64_t high, const void *data);

/*
 * Public function protoypes
 */

/**
  * the function creates a new tree
  *
  * @param tree a pointer to an allocated area of memory for the main
  *  tree data structure.
  * @param comp a pointer to the function to use for comaparing 2 nodes
  *
  * @retval OPAL_SUCCESS if it is successful
  * @retval OPAL_ERR_TEMP_OUT_OF_RESOURCE if unsuccessful
  */
OPAL_DECLSPEC int opal_interval_tree_init(opal_interval_tree_t * tree);


/**
  * inserts a node into the tree
  *
  * @param tree a pointer to the tree data structure
  * @param key the key for the node
  * @param value the value for the node
  *
  * @retval OPAL_SUCCESS
  * @retval OPAL_ERR_TEMP_OUT_OF_RESOURCE if unsuccessful
  */
OPAL_DECLSPEC int opal_interval_tree_insert(opal_interval_tree_t *tree, void *value, uint64_t low, uint64_t high);

/**
  * finds a value in the tree based on the passed key using passed
  * compare function
  *
  * @param tree a pointer to the tree data structure
  * @param key a pointer to the key
  * @param compare function
  *
  * @retval pointer to the value if found
  * @retval NULL if not found
  */
OPAL_DECLSPEC void *opal_interval_tree_find_overlapping (opal_interval_tree_t *tree, uint64_t low, uint64_t high);

/**
  * deletes a node based on its interval
  *
  * @param tree a pointer to the tree data structure
  * @param low low value of interval
  * @param high high value of interval
  *
  * @retval OPAL_SUCCESS if the node is found and deleted
  * @retval OPAL_ERR_NOT_FOUND if the node is not found
  *
  * This function finds and deletes an interval from the tree that exactly matches
  * the given range.
  */
OPAL_DECLSPEC int opal_interval_tree_delete(opal_interval_tree_t *tree, uint64_t low, uint64_t high);

/**
  * frees all the nodes on the tree
  *
  * @param tree a pointer to the tree data structure
  *
  * @retval OPAL_SUCCESS
  */
OPAL_DECLSPEC int opal_interval_tree_destroy(opal_interval_tree_t *tree);

/**
  * traverses the entire tree, performing the cond function on each of the
  * values and if it returns one it performs the action function on the values
  *
  * @param tree a pointer to the tree
  * @param low low value of interval
  * @param high high value of interval
  * @param complete if true only run action function on completely overlapping intervals
  * @param action a pointer to the action function
  *
  * @retval OPAL_SUCCESS
  * @retval OPAL_ERROR if there is an error
  */
OPAL_DECLSPEC int opal_interval_tree_traverse (opal_interval_tree_t *tree, uint64_t low, uint64_t high,
                                               bool complete, opal_interval_tree_action_fn_t action);

/**
  * returns the size of the tree
  *
  * @param tree a pointer to the tree data structure
  *
  * @retval int the nuber of items on the tree
  */
OPAL_DECLSPEC int opal_interval_tree_size (opal_interval_tree_t *tree);

OPAL_DECLSPEC size_t opal_interval_tree_depth (opal_interval_tree_t *tree);

END_C_DECLS
#endif /* OPAL_INTERVAL_TREE_H */

