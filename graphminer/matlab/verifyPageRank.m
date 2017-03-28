function verifyPageRank( A, pC )
    num_nodes = numel(unique([A(:,1);A(:,2)]));
    d = 0.85;
    % Assuming the nodeids are continuous and starts from 1
    % Convert A to matrix form
    Am = zeros(num_nodes*num_nodes,1);
    % Normalize the weights
    [C, IA, IC] = unique(A(:,1));
    W = accumarray(IC, A(:,3));
    Am(sub2ind([num_nodes num_nodes], A(:,1), A(:,2))) = A(:,3)./W(IC);
    Am = reshape(Am, [num_nodes num_nodes]);
    
   
    M = d*Am' + ((1-d)/num_nodes)*ones(num_nodes, num_nodes); 
    % pagerank is the dominant eigen vector of M
    [V,D] = eigs(M,1);

    [x, I] = sort(pC(:,1)); 
    loglog(V,pC(I,2),'.g')
    xlabel('PageRank from Matlab');
    ylabel('PageRank from SQL');
end

