function b_final = verifyFaBP(A, b_prior, b_sql)
    num_nodes = numel(unique([A(:,1);A(:,2)]));
    
    A = [A(:,[1 2]);A(:,[2 1])];
    A = unique(A, 'rows');
    
    % Calculate node degrees
    degree = hist(A(:,1),unique(A(:,1)));
    max_deg = max(degree);
    
    % Calculate homophily factor
    hh_one_norm = (1 / (2 + 2*max_deg));
    
    sum_deg = sum(degree);
    c1= 2+sum_deg;
    sum_deg_sqrd = sum(degree.*degree);
    c2 = sum_deg_sqrd - 1;
    
    hh_frob_norm = sqrt((-c1 + (sqrt(c1*c1 + 4*c2)))/(8*c2));
    
    hh = max(hh_frob_norm,hh_one_norm);
    
    % Calculate c' and a
    k = 4 * hh * hh;
    a = k / (1-k);
    c_prime = (2*hh) / (1-k);
    
    % Convert A to matrix form
    Am = zeros(num_nodes*num_nodes,1);
    Am(sub2ind([num_nodes num_nodes], A(:,1), A(:,2))) = 1;
    Am = reshape(Am, [num_nodes num_nodes]);
    
    D = diag(degree);
    
    % Matrix multiplication
    I_W = eye(num_nodes) + (double(a * D)) - (double (c_prime * Am));
    b_prior_beliefs = b_prior(:,2);
    b_prior_nodeid = b_prior(:,1);
    
    b_final2 = (I_W) \ b_prior_beliefs;
    b_final = [b_prior_nodeid,b_final2];

    % Plot results
    subplot (1,2,1)
    scatter(b_prior_beliefs(:,2),b_prior_beliefs(:,2))
    axis ([-0.015 0.015 -0.015 0.015])
    title 'Prior beliefs'
    subplot (1,2,2)
    scatter(b_final2(:,2),b_sql(:,2))
    axis ([-0.015 0.015 -0.015 0.015])
    title 'Final beliefs using SQL vs MATLAB'
    xlabel 'MATLAB'
    ylabel 'SQL'
end