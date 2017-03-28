function verifyConnectedComponents( A, cC )
    A = [A(:,[1 2]);A(:,[2 1])];
    num_nodes = numel(unique([A(:,1);A(:,2)]));
    % Convert A to matrix form
    Am = zeros(num_nodes*num_nodes,1);
    Am(sub2ind([num_nodes num_nodes], A(:,1), A(:,2))) = 1;
    Am = reshape(Am, [num_nodes num_nodes]);
    Am(eye(num_nodes,num_nodes)==1) = 1;
    C = (1:num_nodes)';
    diff = 1;
    while diff
        M = Am.*repmat(C',[num_nodes 1]);
        Cnew = max(M,[],2);        
        diff = sqrt(sum((Cnew-C).^2));
        C = Cnew;
    end
    
    subplot(1,2,1);
    comps = hist(C, unique(C));
    [c,cx]=hist(comps,unique(comps));
    loglog(cx, c, '--.')
    xlabel 'Component Size';
    ylabel 'Frequency';
    title 'Connected Components';
    
    subplot(1,2,2);
    compsC = hist(cC(:,2), unique(cC(:,2)))';
    [c,cx]=hist(compsC,unique(compsC));
    loglog(cx, c, '--.')
    xlabel 'Component Size';
    ylabel 'Frequency';
    title 'Connected Components from SQL';
    
end

