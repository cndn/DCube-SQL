# -*- coding: utf-8 -*-
# This file contains the various parameters used and default table names

#Initialization database parameters
GM_DB = "postgres"
GM_DB_USER = "postgres"
GM_DB_PASS = "password"
GM_DB_PORT = 5432

#Default Table names
GM_TABLE = "GM_TABLE"
GM_TABLE_UNDIRECT = "GM_TABLE_UNDIRECTED"
GM_NODES = "GM_NODES"

# Degree distribution params
GM_NODE_DEGREES = "GM_NODE_DEGREES"
GM_INDEGREE_DISTRIBUTION = "GM_INDEGREE_DISTRIBUTION"
GM_OUTDEGREE_DISTRIBUTION = "GM_OUTDEGREE_DISTRIBUTION"
GM_DEGREE_DISTRIBUTION = "GM_DEGREE_DISTRIBUTION"

# pagerank
GM_PAGERANK = "GM_PAGERANK"
gm_param_pr_max_iter = 100               # max iterations
gm_param_pr_thres = 0.01              # stopping threshold
gm_param_pr_damping = 0.85              # damping factor

# connected components
GM_CON_COMP = "GM_CON_COMP"

# radius
GM_RADIUS = "GM_RADIUS"
gm_param_radius_max_iter = 50           # max iterations

# eigendecomposition
GM_EIG_VALUES = "GM_EIG_VALUES"
GM_EIG_VECTORS = "GM_EIG_VECTORS"

gm_param_eig_max_iter = 3              # maximun iterations for Lanczos-SO
gm_param_eig_thres1 = 0.1              # threhold for deciding to perform SO
gm_param_eig_thres2 = 0.01             # stopping threshold
gm_param_qr_max_iter = 500               # maximum iterations for QR algorithm
gm_param_qr_thres = 0.01               # QR algorithm stop threshold
gm_param_eig_k = 3                      # number of eigenvectors to return                      

# belief propagation
GM_BELIEF_PRIOR = "GM_BELIEF_PRIOR"
GM_BELIEF = "GM_BELIEF"
gm_param_bp_max_iter = 100               # max iterations
gm_param_bp_thres = 0.01              # stopping threshold

# anomaly detection
GM_EGONET = "GM_EGONET"



