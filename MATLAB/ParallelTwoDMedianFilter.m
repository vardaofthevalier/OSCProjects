function [ outputImage ] = ParallelTwoDMedianFilter(inputImage, windowWidth, windowHeight)
matlabpool open 12

% Get the input image dimensions
[imageHeight, imageWidth, imageDepth] = size(inputImage);
outputImage = zeros(imageHeight, imageWidth, imageDepth);

parfor y = 1:imageHeight 
    % Set initial values for "edges" to keep track of

    if y >= (windowHeight/2) + 1  
    	if y <= imageHeight - (windowHeight/2) + 1
    	% If the window hasn't shifted past the bottom edge of the image
    		top_edge_in_window = 1;
   			bottom_edge_in_window = windowHeight;
    		
   			top_edge_in_image = y - windowHeight/2;
   			bottom_edge_in_image = y + windowHeight/2 - 1;
   		else
    	% Otherwise, if the window has shifted past the bottom edge of the image
    		top_edge_in_window = 1;
   			bottom_edge_in_window = windowHeight - (imageHeight - y) + 1;
    		
   			top_edge_in_image = y;
   			bottom_edge_in_image = imageHeight; 
        end
    else
    % Otherwise, if the window hasn't shifted past the top edge of the image
    	top_edge_in_window = windowHeight/2 + 2 - y;
    	bottom_edge_in_window = windowHeight;
    		
   		top_edge_in_image = 1;
   		bottom_edge_in_image = windowHeight/2 - 1 + y; 
    end
    
    % Along the x-axis:
    left_edge_in_window = windowWidth/2 + 1;
    right_edge_in_window = windowWidth;
    
    left_edge_in_image = 1;
    right_edge_in_image = windowWidth/2;

        % create a new "window" with values to average over, padding the
    for x = 1:imageWidth 
        % sides with zeros for averaging on the image boundary
        
        window = zeros(windowHeight, windowWidth, imageDepth);
		try
			window(top_edge_in_window:bottom_edge_in_window, left_edge_in_window:right_edge_in_window, 1:imageDepth) = inputImage(top_edge_in_image:bottom_edge_in_image, left_edge_in_image:right_edge_in_image, 1:imageDepth);
        catch err
			x
			y
		end
        % find the median of the values in window
        
        for z = 1:imageDepth
		
			sort(window);
            median_value = median(window(1:windowWidth, 1:windowHeight, z));
      
            % insert the median valued point in the appropriate location in outputImage 
     
            outputImage(y, x, z) = median_value;
        end
        
        % Move the window to the right
        
        if x >= (windowWidth/2) + 1
            if x <= imageWidth - (windowWidth/2) + 1
                left_edge_in_window = 1;
                right_edge_in_window = windowWidth;
                
                left_edge_in_image = x - (windowWidth/2);
                right_edge_in_image = x + (windowWidth/2) - 1;
            else
                left_edge_in_window = 1;
                right_edge_in_window = windowWidth - (imageWidth - x) + 1;
                
                left_edge_in_image = x - (windowWidth/2);
                right_edge_in_image = imageWidth;
            end
        else
            left_edge_in_window = (windowWidth/2) + 2 - x;
            right_edge_in_window = windowWidth;
                
            left_edge_in_image = 1;
            right_edge_in_image =  (windowWidth/2) - 1 + x;
        end
            
    end
    
end

end


