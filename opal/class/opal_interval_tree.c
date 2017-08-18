/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2013 The University of Tennessee and The University
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
 */
/*
 * @file
 */

#include "opal_config.h"

#include "opal/class/opal_interval_tree.h"

/* Private functions */
static void btree_insert(opal_interval_tree_t *tree, opal_interval_tree_node_t * node);
static void btree_delete_fixup(opal_interval_tree_t *tree, opal_interval_tree_node_t * x);
static opal_interval_tree_node_t * btree_successor(opal_interval_tree_t * tree,
                                             opal_interval_tree_node_t * node);
static opal_interval_tree_node_t * opal_interval_tree_find_node(opal_interval_tree_t *tree, uint64_t low, uint64_t high, bool exact);
static void left_rotate(opal_interval_tree_t *tree, opal_interval_tree_node_t * x);
static void right_rotate(opal_interval_tree_t *tree, opal_interval_tree_node_t * x);
static void inorder_destroy(opal_interval_tree_t *tree, opal_interval_tree_node_t * node);

#define max(x,y) (((x) > (y)) ? (x) : (y))

/**
 * the constructor function. creates the free list to get the nodes from
 *
 * @param tree the tree that is to be used
 */
static void opal_interval_tree_construct (opal_interval_tree_t *tree)
{
    OBJ_CONSTRUCT(&tree->root, opal_interval_tree_node_t);
    OBJ_CONSTRUCT(&tree->nill, opal_interval_tree_node_t);
    OBJ_CONSTRUCT(&tree->free_list, opal_free_list_t);

    /* initialize sentinel */
    tree->nill.color = BLACK;
    tree->nill.left = tree->nill.right = tree->nill.parent = &tree->nill;
    tree->nill.max = 0;
    tree->nill.ref_count = 1;

    /* initialize root sentinel */
    tree->root.color = BLACK;
    tree->root.left = tree->root.right = tree->root.parent = &tree->nill;
    tree->root.low = (uint64_t) -1;
    tree->root.ref_count = 1;

    /* set the tree size to zero */
    tree->tree_size = 0;
}

/**
 * the destructor function. Free the tree and destroys the free list.
 *
 * @param tree the tree object
 */
static void opal_interval_tree_destruct (opal_interval_tree_t *tree)
{
    opal_interval_tree_destroy (tree);

    OBJ_DESTRUCT(&tree->free_list);
    OBJ_DESTRUCT(&tree->root);
    OBJ_DESTRUCT(&tree->nill);
}

OBJ_CLASS_INSTANCE(opal_interval_tree_t, opal_object_t, opal_interval_tree_construct,
                   opal_interval_tree_destruct);

static void opal_interval_tree_node_construct (opal_interval_tree_node_t *node)
{
    node->ref_count = 0;
    node->lock = 0;
}

OBJ_CLASS_INSTANCE(opal_interval_tree_node_t, opal_free_list_item_t, opal_interval_tree_node_construct, NULL);

static inline void opal_interval_tree_node_lock (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    while (opal_atomic_swap_32 (&tree->lock, 1)) {
        opal_atomic_mb();
    }
}

static inline void opal_interval_tree_node_unlock (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    opal_atomic_wmb ();
    tree->lock = 0;
}

static inline void opal_interval_tree_node_retain (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    (void) opal_atomic_add_32 (&node->ref_count, 1);
}

static inline void opal_interval_tree_node_release (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    int32_t value = opal_atomic_add_32 (&node->ref_count, -1);

    if (0 == value) {
        opal_free_list_return (&tree->free_list, &node->super);
    }
}

/* Create the tree */
int opal_interval_tree_init (opal_interval_tree_t *tree)
{
    return opal_free_list_init (&tree->free_list, sizeof(opal_interval_tree_node_t),
                                opal_cache_line_size, OBJ_CLASS(opal_interval_tree_node_t),
                                0, opal_cache_line_size, 0, -1 , 128, NULL, 0, NULL, NULL, NULL);
}


/* This inserts a node into the tree based on the passed values. */
int opal_interval_tree_insert (opal_interval_tree_t *tree, void *value, uint64_t low, uint64_t high)
{
    opal_interval_tree_node_t * y;
    opal_interval_tree_node_t * node;

    if (low > high) {
        return OPAL_ERR_BAD_PARAM;
    }

    /* get the memory for a node */
    node = (opal_interval_tree_node_t *) opal_free_list_get (&tree->free_list);
    if (OPAL_UNLIKELY(NULL == node)) {
        return OPAL_ERR_OUT_OF_RESOURCE;
    }

    /* insert the data into the node */
    node->data = value;
    node->low = low;
    node->high = high;
    node->max = high;
    node->ref_count = 1;

    /* insert the node into the tree */
    opal_interval_tree_insert_node (tree, node);

    /*do the rotations */
    /* usually one would have to check for NULL, but because of the sentinal,
     * we don't have to   */
    while (node->parent->color == RED) {
        if (node->parent == node->parent->parent->left) {
            y = node->parent->parent->right;
            if (y->color == RED) {
                node->parent->color = BLACK;
                y->color = BLACK;
                node->parent->parent->color = RED;
                node = node->parent->parent;
            } else {
                if (node == node->parent->right) {
                    node = node->parent;
                    left_rotate(tree, node);
                }
                node->parent->color = BLACK;
                node->parent->parent->color = RED;
                right_rotate(tree, node->parent->parent);
            }
        } else {
            y = node->parent->parent->left;
            if (y->color == RED) {
                node->parent->color = BLACK;
                y->color = BLACK;
                node->parent->parent->color = RED;
                node = node->parent->parent;
            } else {
                if (node == node->parent->left) {
                    node = node->parent;
                    right_rotate(tree, node);
                }
                node->parent->color = BLACK;
                node->parent->parent->color = RED;
                left_rotate(tree, node->parent->parent);
            }
        }
    }

    /* after the rotations the root is black */
    tree->root.left->color = BLACK;

    return OPAL_SUCCESS;
}

/* Finds the node in the tree based on the key and returns a pointer
 * to the node. This is a bit a code duplication, but this has to be fast
 * so we go ahead with the duplication */
static opal_interval_tree_node_t *opal_interval_tree_find_node(opal_interval_tree_t *tree, uint64_t low, uint64_t high, bool exact)
{
    opal_interval_tree_node_t *node = &tree->root, *next;
    const opal_interval_tree_node_t *nill = &tree->nill;

    while (nill != node) {
        opal_interval_tree_node_retain (tree, node);

        if ((exact && node->low == low && node->high == high) || (!exact && node->low <= low && node->high >= high)) {
            return node;
        }

        next = (low <= node->low) ? node->left : node->right;

        opal_interval_tree_node_release (tree, node);
        node = next;
    }

    return NULL;
}

void *opal_interval_tree_find_overlapping (opal_interval_tree_t *tree, uint64_t low, uint64_t high)
{
    opal_interval_tree_node_t *node = opal_interval_tree_find_node (tree, low, high, true);
    void *data = node ? node->data : NULL;
    opal_interval_tree_node_release (tree, node);
    return data;
}

static opal_interval_tree_node_t *opal_interval_tree_node_duplicate (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    opal_interval_tree_node_t *new_node = (opal_interval_tree_node_t *) opal_free_list_get (&tree->free_list);

    if (OPAL_LIKELY(NULL != new_node)) {
        new_node->data = node->value;
        new_node->low = node->low;
        new_node->high = node->high;
        new_node->max = node->max;
    }

    return new_node;
}

static void opal_interval_tree_swap_nodes (opal_interval_tree_t *tree, opal_interval_tree_node_t *node1, opal_interval_tree_node_t *node2)
{
    opal_interval_tree_node_t *node2_copy = opal_interval_tree_node_duplicate (node2);
    opal_interval_tree_node_t *parent;

    node2_copy->color = node1->color;
    node2_copy->left = node1->left;
    node2_copy->right = node1->right;
    node2_copy->max = node1->max;

    if (&tree->nill != node1->left) {
        node1->left->parent = node2_copy;
    }
    if (&tree->nill != node1->right) {
        node1->right->parent = node2_copy;
    }

    node2_copy->parent = node1->parent;

    parent = node1->parent;

    if (parent->left == node1) {
        RP_RELEASE(parent->left, node2_copy);
    } else {
        RP_RELEASE(parent->riht, node2_copy);
    }

    opal_interval_tree_node_release (node1);

    parent = node2->parent;
    RP_RELEASE(parent, node2->right);

    opal_interval_tree_node_release (node2);
}

/* Delete a node from the tree based on the key */
int opal_interval_tree_delete(opal_interval_tree_t *tree, uint64_t low, uint64_t high)
{
    opal_interval_tree_node_t *p, *todelete, *y;

    p = opal_interval_tree_find_node (tree, low, high, true);
    if (NULL == p) {
        return OPAL_ERR_NOT_FOUND;
    }

    if ((p->left == &tree->nill) || (p->right == &tree->nill)) {
        todelete = p;
    } else {
        todelete = btree_successor(tree, p);
    }

    y = (todelete->left == &tree->nill) ? todelete->right : todelete->left;

    y->parent = todelete->parent;

    if (y->parent == &tree->root) {
        tree->root.left = y;
    } else {
        if (todelete == todelete->parent->left) {
         todelete->parent->left = y;
        } else {
            todelete->parent->right = y;
        }
    }

    if (todelete != p) {
        p->low = todelete->low;
        p->high = todelete->high;
        p->max = todelete->max;
        p->data = todelete->data;
    }

    if (todelete->color == BLACK) {
        btree_delete_fixup(tree, y);
    }

    opal_free_list_return (&(tree->free_list), &todelete->super);

    opal_atomic_add_size_t (&tree->tree_size, -1);

    return OPAL_SUCCESS;
}

int opal_interval_tree_destroy (opal_interval_tree_t *tree)
{
    /* Recursive inorder traversal for delete */
    inorder_destroy(tree, &tree->root);
    return OPAL_SUCCESS;
}


/* Find the next inorder successor of a node    */

static opal_interval_tree_node_t *btree_successor(opal_interval_tree_t * tree, opal_interval_tree_node_t * node)
{
    opal_interval_tree_node_t * p;

    if (node->right == &tree->nill) {
        p = node->parent;
        while (node == p->right) {
            node = p;
            p = p->parent;
        }

        if (p == &tree->root) {
            return &tree->nill;
        }

        return p;
    }

    p = node->right;
    while (p->left != &tree->nill) {
        p = p->left;
    }

    return p;
}

/* Insert an element in the normal binary search tree fashion    */
/* this function goes through the tree and finds the leaf where
 * the node will be inserted   */
static void opal_interval_tree_insert_node (opal_interval_tree_t *tree, opal_interval_tree_node_t * node)
{
    opal_interval_tree_node_t *n, *parent;

    /* set up initial values for the node */
    node->color = RED;
    node->parent = NULL;
    node->left = &tree->nill;
    node->right = &tree->nill;

    do {
        parent = &tree->root;

        opal_interval_tree_node_retain (parent);
        /* read real root */
        n = parent->left;

        /* find the leaf where we will insert the node */
        while (n != &tree->nill) {
            opal_interval_tree_node_retain (n);
            opal_interval_tree_node_release (parent);
            if (n->max < node->high) {
                n->max = node->high;
            }

            parent = n;
            n = ((node->low < n->low) ? n->left : n->right);
        }

        /* set its parent and children */
        node->parent = parent;

        opal_interval_tree_node_lock (parent);

        /* place it on either the left or the right */
        if ((node->low < parent->low)) {
            ret = opal_atomic_cmpset_ptr (&parent->left, &tree->nill, node);
        } else {
            ret = opal_atomic_cmpset_ptr (&parent->right, &tree->nill, node);
        }

        opal_interval_tree_node_unlock (parent);
        if (!ret) {
            break;
        }
    } while (1);

    (void) opal_atomic_add_size_t (&tree->tree_size, 1);
}

/* Fixup the balance of the btree after deletion    */
static void btree_delete_fixup(opal_interval_tree_t *tree, opal_interval_tree_node_t * x)
{
    opal_interval_tree_node_t *w, *root = tree->root.left;

    while ((x != root) && (x->color == BLACK)) {
        if (x == x->parent->left) {
            w = x->parent->right;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;
                left_rotate(tree, x->parent);
                w = x->parent->right;
            }
            if ((w->left->color == BLACK) && (w->right->color == BLACK)) {
                w->color = RED;
                x = x->parent;
            } else {
                if (w->right->color == BLACK) {
                    w->left->color = BLACK;
                    w->color = RED;
                    right_rotate(tree, w);
                    w = x->parent->right;
                }
                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->right->color = BLACK;
                left_rotate(tree, x->parent);
                x = root;
            }
        } else { /* right    */

            w = x->parent->left;
            if (w->color == RED) {
                w->color = BLACK;
                x->parent->color = RED;
                right_rotate(tree, x->parent);
                w = x->parent->left;
            }
            if ((w->right->color == BLACK) && (w->left->color == BLACK)) {
                w->color = RED;
                x = x->parent;
            } else {
                if (w->left->color == BLACK) {
                    w->right->color = BLACK;
                    w->color = RED;
                    left_rotate(tree, w);
                    w = x->parent->left;
                }
                w->color = x->parent->color;
                x->parent->color = BLACK;
                w->left->color = BLACK;
                right_rotate(tree, x->parent);
                x = root;
            }
        }
    }

    x->color = BLACK;
    return;
}


static void inorder_traversal(opal_interval_tree_t *tree, uint64_t low, uint64_t high,
                              bool complete, opal_interval_tree_action_fn_t action,
                              opal_interval_tree_node_t * node)
{
    if (node == &tree->nill) {
        return;
    }

    inorder_traversal(tree, low, high, complete, action, node->left);

    if ((complete && (low >= node->low && high <= node->high)) ||
        (!complete && ((node->low <= low && low <= node->high) ||
                       (node->low <= high && high <= node->high) ||
                       (low <= node->low && high >= node->high)))) {
        action (node->low, node->high, node->data);
    }

    inorder_traversal(tree, low, high, complete, action, node->right);
}

/* Free the nodes in inorder fashion    */

static void inorder_destroy (opal_interval_tree_t *tree, opal_interval_tree_node_t *node)
{
    if (node == &tree->nill) {
        return;
    }

    inorder_destroy(tree, node->left);
    inorder_destroy(tree, node->right);

    if (node->left != &tree->nill) {
        opal_free_list_return (&tree->free_list, &node->left->super);
    }

    if (node->right != &tree->nill) {
        opal_free_list_return (&tree->free_list, &node->right->super);
    }

    opal_atomic_add_size_t (&tree->tree_size, -1);
}

/* Try to access all the elements of the hashmap conditionally */

int opal_interval_tree_traverse (opal_interval_tree_t *tree, uint64_t low, uint64_t high,
                                 bool complete, opal_interval_tree_action_fn_t action)
{
    if (action == NULL) {
        return OPAL_ERR_BAD_PARAM;
    }

    inorder_traversal (tree, low, high, complete, action, tree->root.left);

    return OPAL_SUCCESS;
}

/* Left rotate the tree    */
/* basically what we want to do is to make x be the left child
 * of its right child    */
static void left_rotate(opal_interval_tree_t *tree, opal_interval_tree_node_t *x)
{
    opal_rb_tree_node_t *right, *x_copy;

    x_copy = opal_interval_tree_node_duplicate (tree, x);
    assert (NULL != x_copy);

    right = x_copy->right;

    x_copy->right = right->left;
    /* x's new parent was its right child */
    x_copy->parent = right;

    /* make the left child of y's parent be x if it is not the sentinal node*/
    if (right->left != tree->nill) {
        right->left->parent = x_copy;
    }

    /* normlly we would have to check to see if we are at the root.
     * however, the root sentinal takes care of it for us */
    if (x == x->parent->left) {
        RP_RELEASE(x->parent->left, right);
    } else {
        RP_RELEASE(x->parent->right, right);
    }

    /* the old parent of x is now y's parent */
    y->parent = x->parent;

    RP_RELEASE(y->left, x_copy);

    opal_interval_tree_node_release (x);
}


/* Right rotate the tree    */
/* basically what we want to do is to make x be the right child
 * of its left child */
static void right_rotate(opal_interval_tree_t *tree, opal_interval_tree_node_t *x)
{
    opal_interval_tree_node_t *left, *right, *x_copy;

    x_copy = opal_interval_tree_node_duplicate (tree, x);
    assert (NULL != x_copy);

    left = x->left;
    right = x->right;

    x_copy->left = left;
    left->parent = x_copy;

    RP_RELEASE(left->right, x_copy);

    x_copy->parent = left;
    x_copy->max = max (x->high, max (left->max, right->max));

    z = x->parent;

    if (z->left == x) {
        RP_RELEASE(z->left, left);
    } else {
        RP_RELEASE(z->right, left);
    }

    left->parent = z;

    opal_interval_tree_node_release (x);
}

/* returns the size of the tree */
int opal_interval_tree_size(opal_interval_tree_t *tree)
{
    return tree->tree_size;
}
