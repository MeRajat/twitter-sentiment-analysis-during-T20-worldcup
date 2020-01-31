import org.apache.spark._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import org.elasticsearch.spark.sql._
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.EsSpark
import java.text.SimpleDateFormat
import org.joda.time.{DateTime, Days}
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SQLContext    
import org.apache.spark.sql.SQLContext._
import scala.collection.mutable.{Map,
      SynchronizedMap, HashMap}
import org.apache.spark.mllib.classification.NaiveBayes
import java.util.Properties
import java.io.File
import org.apache.spark.mllib.regression.LabeledPoint
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations
import edu.stanford.nlp.pipeline.{Annotation, StanfordCoreNLP}
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations
import org.apache.spark.mllib.evaluation.MulticlassMetrics

import scala.collection.convert.wrapAll._
import org.apache.spark.mllib.feature.HashingTF
/**
 * Performs Twitter sentiment analysis.
 * Creates a tuple of (positive, negative, neutral) sentiment and combines them in a window of 10 seconds.
 */
object TwitterSentimentAnalysis {

  def main(args: Array[String]) {


  //     if (args.length < 2) {
  //     System.err.println("Usage: TwitterSentimentAnalysis <Team 1 Name> <Team 2 Name> [<filters>]")
  //     System.exit(1)
  //   }



  //   val Array(team1, team2) = args.take(2)
  // //  val filters = args.takeRight(args.length - 2)
  val team1 = "india"
  val team2 = "westindies"
  val match1=team1+"vs"+team2

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
   System.setProperty("twitter4j.oauth.consumerKey", "blah_blah")
    System.setProperty("twitter4j.oauth.consumerSecret", "blah_blah")
    System.setProperty("twitter4j.oauth.accessToken", "blah_blah")
    System.setProperty("twitter4j.oauth.accessTokenSecret", "blah_blah")
    var filters = Array("#IndvsWI","#Ind","#WI","#WIvsInd","#IndvWI","#WIvInd","#INDvWI","#WCT20","#WT20","dhoni","kohli","gayle","bumrah","raina","ashwin","yuvraj","rahane","sammy","samuel","bravo","pandya","jadeja","russell") 

    val sparkConf = new SparkConf().setAppName("TwitterSentimentAnalysis").setMaster("local[*]") 

    
    // sparkConf.set("es.nodes", "localhost")
    // sparkConf.set("es.port", "9200")



   sparkConf.set("es.nodes", "52.86.189.186")
   sparkConf.set("es.port", "9200")


    // sparkConf.set("es.nodes", "search-tweets-xdj7mhlcggqn5cas56fpcn66me.us-east-1.es.amazonaws.com")
    // sparkConf.set("es.port", "80")
    sparkConf.set("es.resource",match1+"/tweets")
    sparkConf.set("es.index.auto.create", "true")
    sparkConf.set("es.nodes.wan.only", "true")

    @transient val streamingCtxt = new StreamingContext(sparkConf, Seconds(5))
    @transient val twitterStream = TwitterUtils.createStream(streamingCtxt,None, filters)

   
    val sc = SparkContext.getOrCreate

    val sqlContext = new SQLContext(sc)

    var htf = new HashingTF(900)
    val positiveData = sc.textFile("src/main/resources/pos.txt")
      .map { text => new LabeledPoint(2, htf.transform(text))}
    val neutralData = sc.textFile("src/main/resources/neutral.txt")
      .map { text => new LabeledPoint(1, htf.transform(text))}
    val negativeData = sc.textFile("src/main/resources/neg.txt")
      .map { text => new LabeledPoint(0, htf.transform(text))}
    val d =  positiveData.union(negativeData)
    val training = d.union(neutralData)
    val model = NaiveBayes.train(training)

// val team1Set = Set("england","eng","english","@Eoin16","eoin","morgan","e.morgan","@MoeenAli","moeen","ali","m.ali",
//       "@sambillings","sam","billings","s.billings","@josbuttler","jos","buttler","j.buttler","@daws128","liam,dawson","l.dawson",
//       "@AlexHales1","alex","hales","a.hales","@CJordan","chris","jordan","c.jordan","@Liam628","liam","plunkett","l.plunkett",
//       "@AdilRashid03","adil","rashid","a.rashid","@root66","joe","root","j.root","@JasonRoy20","jason","roy","j.roy",
//       "@benstokes38","ben","stokes","b.stokes","@reece_topley","reece","topley","r.topley","@vincy14","j.vince","vince",
//      " @david_willey","d.willey","willey"

//   )













    val team1Set = Set("IND","india","meninblue","@msdhoni","msd","dhoni","mahi","captaincool",
      "@imVkohli","cheeku","kohli","virat","v.kohli","@ashwinravi99","ashwin","r.ashwin","raviashwin",
      "@ImRaina","raina","s.raina","suresh","@ajinkyarahane88","rahane","a.rahane","@SDhawan25","dhawan","shikhar",
      "@Guru_A_Nehra","nehra","ashish","happydent","@jasprit_bumrah","bumrah","jasprit","jbumrah","j.bumrah",
      "@hardikpandya7","pandya","hardik","h.pandya","@YUVISTRONG12","yuvi","yuvraj","y.singh","@harbhajan_singh","bhajji","harbhajan,h.singh",
      "@imjadeja","jaddu","jadeja","r.jadeja","ravindra","@sachin_rt","sachin,tendulkar","s.tendulkar","@ImIshant","ishant","i.sharma",
      "@ImRo45","rohit","r.sharma","rohitsharma","@ImZaheer","zaheer","z.khan")
    val team2Set = Set("west indies","wi","indies","@darrensammy88","darren","sammy","d.sammy","@qmanbad","samuel","badree","s.badree",
      "@suliebenn","benn","sulieman","s.benn","@TridentSportsX","carlos","brathwaite","@DJBravo47","dwayne","bravo","DJ bravo","d.bravo",
      "johnson","charles","j.charles","andre","fletcher","a.fletcher","@henrygayle","chris","gayle","c.gayle","gaylestorm","gayle storm"
      "@Jaseholder98","jason","holder","j.holder","ashley","nurse","a.nurse","@shotta8080","denesh","ramdin","d.ramdin",
      "@Russell12A","andre","russell","a.russell","@MarlonSamuels7","samuels","marlon","m.samuels","@jerometaylor75","jerometaylor",
      "jerome","j.taylor","evin","lewis","e.lewis"


)


    val data = twitterStream.map(tweet => {
   // tweet.saveAsTextFile(tweets);


     val formatter = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZ")
  

      def checkForData ( x : Any ) : String ={
        if (x!= null)
          return x.toString  
        else 
          return "N.A."

      }

      def getAnalysis ( x : String ) : String = {

      val hashed = htf.transform(x)
      val score1 = model.predict(hashed)
      var score2 = 0
         val props = new Properties()
        props.setProperty("annotators", "tokenize, ssplit, parse, sentiment")
        val pipeline: StanfordCoreNLP = new StanfordCoreNLP(props)
        var resultingTweet = x.replaceAll("[^a-zA-Z\\s]", "")
        val annotation = pipeline.process(resultingTweet)
        val sentences = annotation.get(classOf[CoreAnnotations.SentencesAnnotation])
        val resultList =  sentences.map(sentence => (sentence, sentence.get(classOf[SentimentCoreAnnotations.SentimentAnnotatedTree])))
        .map { case (sentence, tree) => (sentence.toString, RNNCoreAnnotations.getPredictedClass(tree)) }
        .toList
        var sum = 0
        for(x <- 0 to resultList.length-1) {
            sum = sum + resultList(x)._2
        } 
        var result = sum/resultList.length
        if (result >=3 && result <= 4)
            {
              score2=2
            }
        else if (result >= 0 && result <= 1) 
            {score2=0} 
        else 
            {score2=1}


        val score = score2 + score1



        if (score==0)
            return "ExtremeNegative"
        else if(score==1.0)
            return "MildlyNegative"
        else if(score==2.0)
            return "Neutral"
        else if(score==3.0)
            return "MildlyPositive"
        else 
            return "ExtremePositive"

      }


      def checkIsRetweeted ( x : String ) : String = {

        if(x.startsWith("RT"))
          return "Yes"
        else
          return "No"

      }

    def checkSource (x : String) : String  = {
        if(x.contains("Instagram"))
            return "Instagram"
        else if(x.contains("Web"))
            return "Web"
        else if(x.contains("iPad"))
            return "iPad"
        else if(x.contains("iPhone"))
            return "iPhone"
        else if(x.contains("Android"))
            return "Android"
        else if(x.contains("Windows Phone"))
            return "WindowsPhone"
        else if(x.contains("iΟS"))
            return "iΟS"
        else 
            return "Others"

      }

      def checkLanguage ( x : String ) : String = {
         if(x.equals("en"))
            return "English"
        else if(x.equals("hi"))
            return "Hindi"
        else if(x.equals("ar"))
            return "Arabic"
        else if(x.equals("bn"))
            return "Bengali"
        else if(x.equals("es"))
            return "Spanish"
        else if(x.equals("fr"))
            return "French"
        else if(x.equals("Web"))
            return "Web"
        else if(x.equals("it"))
            return "Italian"
        else if(x.equals("ja"))
            return "Japanese"
        else if(x.equals("ml"))
            return "Malayalam"
        else if(x.equals("mr"))
            return "Marathi"
        else if(x.equals("ms"))
            return "Malay"
         else if(x.equals("ta"))
            return "Tamil"           
         else if(x.equals("te"))
            return "Telegu"   
         else if(x.equals("ur"))
            return "Urdu"   
         else if(x.equals("zh"))
            return "Chinese"   
         else
            return "Others"                           
   
      }

     val computeCount = (words: Array[String], set: Set[String]) => {
      var result = 0
      for (w <- words) {
        if (set.contains(w)) {
          result += 1
        }
      }
      result
    }


    def getSupport ( x : String , sentiment : String) : String = {
            
        if(sentiment.equals("Neutral"))
            {
                return "Neutral"
            }

        else {

        val splitWords = x.split(" ")
        val team1Favour = computeCount(splitWords, team1Set)
        val team2Favour = computeCount(splitWords, team2Set)
        val score = team1Favour - team2Favour
        if (score > 0) 
            {   
                if(sentiment.equals("Positive") || sentiment.equals("MildlyPositive"))
                {
                    return team1
                }
                else
                {   
                return team2
                }   
            } 
        else if (score < 0) 
            {
                if(sentiment.equals("Positive") || sentiment.equals("MildlyPositive"))
                {
                    return team2
                }
                else
                {   
                return team1
                }   
            } 
        else {
                return "Neutral"
            }

        }


    }

    val tweetText = tweet.getText
    val resultOfAnalysis = getAnalysis(tweetText)
    val supportAnalysed = getSupport(tweetText,resultOfAnalysis)

HashMap(            
       "TweetID" -> tweet.getId,
        "StatusCreatedAt" -> formatter.format(tweet.getCreatedAt.getTime) ,
        "Tweet" -> tweetText , //tweet.getText,
        "Source" -> checkSource(tweet.getSource) , 
     //   "RetweetCount" -> tweet.getRetweetCount , 
        "UserID" -> tweet.getUser.getId ,
        "UserScreenName" -> tweet.getUser.getScreenName  ,
        "UserName" -> tweet.getUser.getName  ,
          "UserFriendsCount" -> tweet.getUser.getFriendsCount,
          "UserFavouritesCount" -> tweet.getUser.getFavouritesCount ,
          "UserFollowersCount" -> tweet.getUser.getFollowersCount ,
        "UserFollowersRatio" -> tweet.getUser.getFollowersCount.toFloat/tweet.getUser.getFriendsCount.toFloat ,
        "UserTweetCount" -> tweet.getUser.getStatusesCount,
          "UserLang" -> checkLanguage(tweet.getUser.getLang), 
          "UserLocation" -> checkForData(tweet.getUser.getLocation),
          "UserTimeZone" -> checkForData(tweet.getUser.getTimeZone),
         "isRetweeted" -> checkIsRetweeted(tweet.getText) , 
         "SentimentAnalyzed" -> resultOfAnalysis, //getAnalysis(tweet.getText)
         "UserSupports" -> supportAnalysed

       ) 


      })



   var outputDirectory="tweet"

    val outputDir = new File(outputDirectory.toString)



data.foreachRDD{rdd => {


    EsSpark.saveToEs(rdd,match1+"/tweets", Map("es.mapping.timestamp" -> "StatusCreatedAt"))
  }}

    streamingCtxt.start()
    streamingCtxt.awaitTermination()
   }
}
