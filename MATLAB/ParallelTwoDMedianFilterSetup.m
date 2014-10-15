% Entry point for 2D median filter application
% This script collects the image and sliding window dimensions from the 
% user, filters the image and then displays the corrected image.

%imagePrompt = 'Enter an absolute path name for an image file: ';
%heightPrompt = 'Enter the window height: ';
%widthPrompt = 'Enter the window width: ';

%imageLocation = input(imagePrompt, 's');
%height = input(heightPrompt, 's');
%width = input(widthPrompt, 's');
timer_start = tic;

imageLocation = '/nfs/06/ahahn/local/scripts/matlab/PCT_training/abbyandchris.JPG';
windowHeight = 20;
windowWidth = 20;

inputImage = imread(imageLocation);
noisyImage = imnoise(inputImage, 'salt & pepper');

%windowHeight = str2num(height);
%windowWidth = str2num(width);

% For the purposes of efficiency of the parallel algorithm, determine the
% larger of the two dimensions -- the algorithm will parallelize along this
% dimension

[imageHeight, imageWidth, imageDepth] = size(inputImage);

if imageWidth > imageHeight
    imrotate(inputImage, 90);
    isRotated = true;
end

outputImage = ParallelTwoDMedianFilter(inputImage, windowWidth, windowHeight);

if isRotated
    imrotate(outputImage, -90);
end

imwrite(outputImage, 'abbyandchriscorrected.jpg');

elapsed_time = toc(timer_start)




