function detectAnomaly( degree, egonet, k1, k2 )
    [x idx1] = sort(degree(:,1));
    [x idx2] = sort(egonet(:,1));
    
    find(degree(idx1,1)~=egonet(idx2,1))
    
    data = [degree(idx1,2)+1 egonet(idx2,2) egonet(idx2,3)];
    
    P1 = polyfit(log(data(:,1)),log(data(:,2)),1)
    P2 = polyfit(log(data(:,2)),log(data(:,3)),1)
    yfit1 = exp(P1(1)*log(data(:,1)) + P1(2));
    yfit2 = exp(P2(1)*log(data(:,2)) + P2(2));
    
    out1 = (max(data(:,2), yfit1)./min(data(:,2), yfit1)).* log(abs(data(:,2) - yfit1) + 1);
    out2 = (max(data(:,3), yfit2)./min(data(:,3), yfit2)).* log(abs(data(:,3) - yfit2) + 1);
    
    fhd = figure;
    hd = subplot(1,2,1, 'FontSize',7);
    loglog(data(:,1), data(:,2),'g.');
    hold on;
    %loglog(data(out1>thr,1), data(out1>thr,2),'.black');
    [x idx1] = sort(out1,1,'descend');
    idx1 = idx1(1:k1);
    loglog(data(idx1,1), data(idx1,2),'.black');
   
    rng = [2 max(data(:,1))];
    h1 = loglog(rng, exp(P1(1)*log(rng) + P1(2)), '-r');  
    h2 = loglog(rng, rng-1,'-.b');
    h3 = loglog(rng, rng.*(rng-1),':b');
    legend([h1 h2 h3],sprintf('Best fit : Slope = %3.2f', P1(1)),sprintf('Star Egonet'),sprintf('Clique Egonet'));
    
    xlabel ('Number of nodes');
    ylabel ('Number of edges');
    hold off;
    set(hd, 'Box', 'off', 'TickDir', 'out','TickLength', [.03 .03],'XMinorTick', 'on', ...
            'YMinorTick','on','YGrid', 'off','XColor', [.3 .3 .3], 'YColor', [.3 .3 .3], 'Color',[1 1 1]);
        
    hd = subplot(1,2,2, 'FontSize',7);
    loglog(data(:,2), data(:,3),'g.'); 
    hold on;
    %loglog(data(out2>thr,2), data(out2>thr,3),'.black');
    [x idx1] = sort(out2,1,'descend');
    idx1 = idx1(1:k2);
    loglog(data(idx1,2), data(idx1,3),'.black');
    
    rng = [2 max(data(:,2))];
    h1 = loglog(rng, exp(P2(1)*log(rng) + P2(2)), '-r');  
    legend(h1,sprintf('Best fit : Slope = %3.2f', P2(1)));
    
    xlabel ('Number of edges');
    ylabel ('Total weight');
    hold off;
    set(hd, 'Box', 'off', 'TickDir', 'out','TickLength', [.03 .03],'XMinorTick', 'on', ...
            'YMinorTick','on','YGrid', 'off','XColor', [.3 .3 .3], 'YColor', [.3 .3 .3], 'Color',[1 1 1]);

    set(findall(fhd,'type','text'),'fontSize',7);
end

