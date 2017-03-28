function [d e] = verifyAnomalyDetection( Ad)
   % Compute the degrees from adjacency mat, A
   A = [Ad; Ad(:,[2 1 3])];
   degree = hist(A(:,1), unique(A(:,1)));
   
   d = [unique(A(:,1)) degree'];
   
   num_nodes = numel(unique([A(:,1);A(:,2)]));
   Am = zeros(num_nodes*num_nodes,1);
   Am(sub2ind([num_nodes num_nodes], A(:,1), A(:,2))) = 1;
   Am = reshape(Am, [num_nodes num_nodes]);
    
   edge_cnt = [];
   edge_wt = [];
   
   for i=1:size(A,1)
      nodes = find(bitand(Am(A(i,1),:),Am(A(i,2),:)))';
      edge_wt = [edge_wt; [nodes repmat([A(i,3)/2 1/2], [size(nodes,1) 1])]];
   end
 
   edge_wt = [edge_wt; [A(:,1) A(:,3) ones(size(A,1),1)]];
   
   
   
   [C, IA, IC] = unique(edge_wt(:,1));
   ecnt = accumarray(IC,edge_wt(:,3));
   ewt = accumarray(IC,edge_wt(:,2));
   
   e = [C ecnt ewt];
   
   
end

