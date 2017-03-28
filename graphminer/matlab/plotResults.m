dirpath = '../datasets/';
datasets =  {{'Social','Twitter Social','1_social_twitter',1,'g'},...
             {'Social','Youtube Social','7_social_youtube',0,'g'},...
             {'Co-occurence','Amazon Co-occurence','2_co-occur_amazon',1,'r'},...
             {'Co-occurence','Flickr Co-occurence','3_co-occur_flickr',0,'r'},...
             {'Reference','Google Reference','4_reference_google',1,'b'},...
             {'Reference','Patents Reference','8_reference_patents',1,'b'},...
             {'Ratings','Stack Overflow Ratings','5_ratings_SO',1,'y'},...
             {'Ratings','Amazon Ratings','6_ratings_amazon',1,'y'},...  
             {'Physical','Skitter Physical','9_physical_skitter',0,'m'}};
        
tasks =     {{'In Degree Distribution', 'indegreedist.csv'},...
             {'Out Degree Distribution', 'outdegreedist.csv'},...
             {'Degree Distribution', 'degreedist.csv'},...
             {'Page Rank', 'pagerank.csv'},...
             {'In-degree vs PageRank', 'pagerank.csv', 'degree.csv'},...
             {'Out-degree vs PageRank', 'pagerank.csv', 'degree.csv'},...
             {'Weakly Connected Components','conncomp.csv'},...
             {'Radius','radius.csv'},...
             {'Radius vs Pagerank','radius.csv','pagerank.csv'},...
             {'Eigenvectors','eigvec.csv'},...
             {'Triangle count','eigval.csv','degree.csv'}};
         
for i=[10]
    display(['Perfroming task ' tasks{i}{1}]);
    fhd = figure('Color',[1 1 1]);
    
    cnt = [];
    pcnt = [];
   
      
    for j=[8]
        file = [dirpath datasets{j}{3} '/' tasks{i}{2}];
        display(['Loading file ' file]);
        data = importdata(file);
        c = datasets{j}{5};
        hd = subplot(3,3,j, 'FontSize',9);
        l = 0;
        if (i<=3)
           % Degree dists
           avgd = sum(data(:,1).*data(:,2))/sum(data(:,2));
           zerod = data(find(data(:,1)==0),2);
           maxd = max(data(:,1));
           loglog(data(:,1), data(:,2), ['.' c]); 
           if zerod
               leg = sprintf('Avg. degree=%5.3f\nMax degree=%d\nZero degree Count=%d',avgd, maxd, zerod);
           else
               leg = sprintf('Avg. degree=%5.3f\nMax degree=%d',avgd, maxd);
           end
           l = legend(leg); 
        elseif(i==4)
            % Page Rank
            [hst hstx] = hist(data(:,2),1000);
            loglog(hstx, hst, ['.' c]);
        elseif(i==5)
            % Page Rank vs In-degree
            file = [dirpath datasets{j}{3} '/' tasks{i}{3}];
            data2 = importdata(file);
            plot(data(:,2),data2(:,2),['.' c]);
            clear data2;
        elseif(i==6)
            % Page Rank vs Out-degree
            file = [dirpath datasets{j}{3} '/' tasks{i}{3}];
            data2 = importdata(file);
            plot(data(:,2),data2(:,3),['.' c]);
            clear data2;
        elseif(i==7)
            % Connected Components
            cd = hist(data(:,2),unique(data(:,2)));            
            [hst hstx] = hist(cd, unique(cd));
            loglog(hstx, hst, ['.' c]);
            l = legend(sprintf('Largest Conn. Comp=%d\nNumber of Components=%d',max(cd),size(cd,2))); 
        elseif(i==8)
            % Radius
            [hst hstx] = hist(data(:,2),unique(data(:,2)));          
            semilogy(hstx, hst, ['->' c], 'MarkerSize',4, 'MarkerFaceColor',c, 'LineWidth', 2);
            l = legend(sprintf('Avg. Radius=%5.3f',mean(data(:,2)))); 
        elseif(i==9)
            % Radius vs pagerank
            file = [dirpath datasets{j}{3} '/' tasks{i}{3}];
            data2 = importdata(file);
            plot(data(:,2),data2(:,2),['.' c]);
            l = legend(sprintf('Avg. Radius=%5.3f',mean(data(:,2)))); 
            clear data2;
        elseif(i==10)
            % Eigenvectors
            num = size(data,1)/4;
            minerr = Inf;
            e = [0 1];
            for k1=0:2
                e1 = abs(data(k1*num+1:(k1+1)*num,3));
                for k2=k1+1:3
                    e2 = abs(data(k2*num+1:(k2+1)*num,3));
                    err = abs(corr2(e1,e2));
                    if (err<minerr)
                        minerr = err;
                        e = [k1 k2];
                    end
                end
            end
            vec1 = data(e(1)*num+1:(e(1)+1)*num,3);
            vec2 = data(e(2)*num+1:(e(2)+1)*num,3);
            plot(vec1(vec1<0.2 & vec2<0.2,:), vec2(vec1<0.2 & vec2<0.2,:),['.' c]);
            l = legend(sprintf('EV %d vs EV %d',e(2)+1, e(1)+1)); 
            clear vec1;
            clear vec2;
            clear e1;
            clear e2;
        elseif(i==11)
            % Triangle count
            file = [dirpath datasets{j}{3} '/' tasks{i}{3}];
            data2 = importdata(file);
            num_nodes = size(data2,1);
            cnt = [cnt; sum(data(:,2).^3)/6];
            pcnt = [pcnt; cnt(end,1)/num_nodes];
            clear data2;
        end
        title(datasets{j}{2});
        if l
            lh2 = get(l,'children');delete(lh2(1));delete(lh2(2)); legend ('boxoff'); gt1=findobj(l,'type','text');set(gt1,'color',[0.8 0.8 0.8]);
        end
        clear l;
        set(hd, 'Box', 'off', 'TickDir', 'out','TickLength', [.03 .03],'XMinorTick', 'on', ...
            'YMinorTick','on','YGrid', 'off','XColor', [.3 .3 .3], 'YColor', [.3 .3 .3], 'Color',[0.4 0.4 0.4]);
    end
    if (i==11)
        hd = subplot(1,1,1);
        legs = {};
        for k=1:size(datasets,2)
            legs{k} = [datasets{k}{2} sprintf('\nCount = %d, Count/n = %5.2f',round(cnt(k)),pcnt(k))];
            br = bar(k,cnt(k), 'BarWidth',0.4);
            hold on;
            set(br,'FaceColor', datasets{k}{5});
        end
        hold off;
        set(gca,'YScale','log');
        l = legend(legs,'Location','SouthEastOutside'); 
        set(hd, 'Box', 'off', 'TickDir', 'out','TickLength', [.03 .03],'XMinorTick', 'on',...
            'YMinorTick','on','YGrid', 'off','XColor', [.3 .3 .3], 'YColor', [.3 .3 .3]);
    end    
    
    %suptitle(tasks{i}{1});
    set(findall(fhd,'type','text'),'fontSize',9);
end
clear data;
         
         
         