# CollaborativeFilteringRecommendation

This project builds a Collaborative Filtering Recommendation Engine using MapReduce and Hadoop.

********Original Author: Pratima Kshetry******************************************************************************************
University of Maryland, College Park
Technologies Used: Hadoop, MapReduce, Java
**************************************************************************************************************************************

################################################################################
*******************************************************Folder Structure***************************************************************

------The MapReduce folder contains all the source code.

------The ClusterCorrelation folder contains output files that were generated after generating cluster points from the amazon data.

------The CustomerProfile folder contains all the customer profiles data generated from the data.

------The EvaluationDataFiles folder contains data that were used for evaluation. Also read ReadMe.md of this folder for further info.

------The ProductProfile folder contains all the product profiles generated from the data.

------The Ranking folder contains the output of the ranking of products obtained after correlating the products.

------The Scammers folder contains the user logs that have been classified as Scammers-NonScammers.

------The Screenshots folder contains few screenshots of the system
------The SystemDesign document constains the design document. you may like to check this folder to learn about the design of the system.

Browse to specific folders to see the output generated after each task was performed. The naming conventions for these output folder follow the type of output it contains. For example, the output for product profile is contained in the folder ProductProfile.

Evaluation of the Recommender System:
for evaluation, I have created two output files include and exclude.Include is the training data while exclude is the test data. further explanations on how this recommender system has been evaluated can be found in the ReadMe.md file inside the EvaluationDataFiles folder.


