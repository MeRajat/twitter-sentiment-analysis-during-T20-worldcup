# Twitter-Sentiment analysis
Perform twitter sentiment analysis using Apache Spark Streaming. 

[![Build Status](https://travis-ci.org/rahulsh1/twitter-sentiment-analysis.svg?branch=master)](https://travis-ci.org/rahulsh1/twitter-sentiment-analysis)

This scala program accumulates count of positive, negative and neutral sentiment tweets and print out them every 10 seconds with 10 second sliding window
    

### You need
 * Intelli-J with Scala plugin
 * Registered a Twitter application at https://apps.twitter.com and sign in with your twitter account and created an application.<br/>
   You will get \<consumer key> \<consumer secret> \<access token> \<access token secret>
   
### Configuration
 * Import the project as SBT project
 * It uses scala version: `2.10.4` and Spark version: `1.4.1`. Update this if required inside build.sbt
 * Goto Run -> Edit configuration -> Program Arguments
 * Enter 4 params: consumerKey consumerSecret accessToken accessTokenSecret

### To Run
  * Run -> Run TwitterSentimentAnalysis

### Sample output
    Tweet:Currently watching Beauty Beast 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:The Best  Bodybuilding Supplements For Hardgainers
    httpstcoVsjveOOcPH httpstcovbbCCbz 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT yukifurukawaHP  staffh 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT CraveToRave You run  httpstcolpOTBeni 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT kordeimanis meggings wow masculinity fragile word leggings girls https 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:kbdpftcris lepitennicell PushAwardsKathNiels 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT TvlertheCreator Basically httpstcoheSUcBjQEe 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT Zaferabbas   love a friend 	 Pos:1	 Neg:0	 Neutral:0
    Tweet:BindaasChokri craigfonseca WeirdlyWiredWeedTea 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:ugh 	 Pos:0	 Neg:1	 Neutral:0
    Tweet:RT AreaTawa Nyesek itu Gitu cewe naik Timbangan eh timbangan nya ngomong Maap mbak untuk timbangan gajah disebelah yah 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT RANNMALHOTRA The Guy nervous nervousGreat work Rep Alan Grayson
    httpstcoEkjZSRApvY 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:Matulog na pls 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT EoNGeoNGZZ 
     httpstcoTXERYmkBA 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT themusicnetwork FREEumusicAU teamed MMADAustralia a nationwide campaign single MMADU httpstcohyamHwGG h 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT Georgeiloka Smh httpstcoOjvwUFSU 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT janinist The amount hate Muslims directed hashtag ExMuslimBecause shows persecution ExMuslims faced Mu 	 Pos:0	 Neg:1	 Neutral:0
    Tweet:kbdpftcris kbdpftcris knteroristard PushAwardsKathNiels PushAwardsKathNiels 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:Dapat pala sa bahay nalang ako 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT CoolPetVideo Hedgehog Vines  Cute Hedgehogs That Run Hide Play Eat   httpstcotgEWmSI 	 Pos:0	 Neg:0	 Neutral:1
    
    Twitter Sentiment in last 10 seconds
    Positive: 12 Negative:9 Neutral:95
    Tweet:Thanks top interactors PletchaPJWebb GERARDJAMESHAY Backpacklaptop   tweetjukebox httpstcoUXJlUhoO 	 Pos:1	 Neg:0	 Neutral:0
    Tweet:K Return Kings Episode  Subtitle Indonesia httpstcoCFTRjHvLNU 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:Now 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT CaseThurmon Thank RebelNation time West Monroe Greatest memories life 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:os ALDUBSumptuousLunch 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT thefemaleboook Life goals  httpstconfXdPrNL 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT Iifepost heart  httpstcolKLSedERg 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:This cute  httpstcoSWvvPKM 	 Pos:1	 Neg:0	 Neutral:0
    Tweet:zaweiner avi cute goodness I love 	 Pos:1	 Neg:0	 Neutral:0
    Tweet:NizamTyra tak boleh tak boleh tak bolehheee cubit kang 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:Im earning mPLUSRewards mPLACES httpstcofSNXhWLRf 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT ALDUBEUROPE Kulit ni Meng Shes naturally jolly mainedcm 
    ALDUBSumptuousLunch httpstcovpbAsdVOoQ 	 Pos:1	 Neg:0	 Neutral:0
    Tweet:hunger satisfied 	 Pos:1	 Neg:0	 Neutral:0
    Tweet:RT LDSquotable Put God trials face  Koichi Aoyagi 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT FlirtyNotes My mother a blessing 	 Pos:1	 Neg:0	 Neutral:0
    Tweet:RT AlexHirsch DavePetillo We Didnt Start Cipher 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT MadanChikna Was watching Prem Ratan Dhan Payo
    please  MemoriesIWantToDelete httpstcoClYDHhPyy 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:Without time 	 Pos:0	 Neg:0	 Neutral:1
    Tweet:RT OneFunnyMummy Yet Such a small word huge possibilities 	 Pos:0	 Neg:0	 Neutral:1
    
    Twitter Sentiment in last 10 seconds
    Positive: 39 Negative:19 Neutral:146


### License
MIT

