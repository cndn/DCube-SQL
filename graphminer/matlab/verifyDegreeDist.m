function verifyDegreeDist( A, dC, idC, odC )
   % Compute the degrees from adjacency mat, A
   num_nodes = numel(unique([A(:,1);A(:,2)]));
   out_degree = hist(A(:,1), num_nodes)';
   in_degree = hist(A(:,2), num_nodes)';
   degree = hist([A(:,1);A(:,2)], num_nodes)';
   
   % Get degree distribution
   [d,dx]=hist(degree,unique(degree));
   [od,odx]=hist(out_degree,unique(out_degree));
   [id,idx]=hist(in_degree,unique(in_degree));
   
   subplot(1,3,1);
   loglog(odx*2,od,'.',odC(:,1), odC(:,2),'.');
   title 'OutDegree';
   subplot(1,3,2);
   loglog(idx*2,id,'.',idC(:,1), idC(:,2),'.');
   title 'InDegree';
   subplot(1,3,3);
   loglog(dx*2,d,'.',dC(:,1), dC(:,2),'.');
   title 'Degree';
end

