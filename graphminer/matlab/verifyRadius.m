function [R] = verifyRadius( A, rC )
    A = [A(:,[1 2]);A(:,[2 1])];
    num_nodes = numel(unique([A(:,1);A(:,2)]));
    % Convert A to matrix form
    Am = zeros(num_nodes*num_nodes,1);
    Am(sub2ind([num_nodes num_nodes], A(:,1), A(:,2))) = 1;
    Am = reshape(Am, [num_nodes num_nodes]);
    Am(eye(num_nodes,num_nodes)==1) = 1;
    
    R = -1*ones(num_nodes,1);
    Cprev = eye(num_nodes,num_nodes);
    curR = 0;
    
    diff = 1;
    while diff
        C = [];
        for i=1:num_nodes
            C = [C max(Cprev(:,Am(i,:)==1),[],2)];
        end
        R((sum(C - Cprev)==0) & R'==-1) = curR;
        diff = sum(sum(C - Cprev))
        curR = curR + 1;
        Cprev = C;
    end
    
    subplot(1,2,1);
    hist(R,unique(R));
    title 'Actual Radius';
    subplot(1,2,2);
    hist(rC(:,2),unique(rC(:,2)));
    title 'Approx Effective Radius';
end

