/**
  * Created by gilcu2 on 12/24/16.
  */

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth
import scala.collection.immutable.HashSet

object AssociationRules {

  case class RetweetFromTo(retweeter: String, retweeted: String)

  def main(args: Array[String]): Unit = {

    if (args.length < 4) {
      System.err.println("Usage: TwitterPopularTags <consumer key> <consumer secret> " +
        "<access token> <access token secret> [<filters>]")
      System.exit(1)
    }

    val Array(consumerKey, consumerSecret, accessToken, accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    // Set the system properties so that Twitter4j library used by twitter stream
    // can use them to generate OAuth credentials
    System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken", accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

    val sparkConfiguration = new SparkConf()
      .setAppName("AssociationRules")
      .setMaster("local[*]")

    // Let's create the Spark Context using the configuration we just created
    val sparkContext = new SparkContext(sparkConfiguration)

    // Now let's wrap the context in a streaming one, passing along the window size
    val ssc = new StreamingContext(sparkContext, Seconds(30))
    ssc.checkpoint("/tmp/streaming")

    val status = TwitterUtils.createStream(ssc, None, filters)
    val wordsTwitter = filters.toSet

    val tweets = status.filter(x => !x.isRetweet)

    val texts = tweets.map(x => {
      val doc = x.getText
      val words = doc.split(rege).map(_.toLowerCase)
      val filtered = words
        .filter(_.length > 2)
        //        .filter(x=> ! wordsTwitter.contains(x))
        .filter(x => !stopWords.contains(x)).distinct.toVector
      val userLogin = x.getUser.getScreenName
      if (filtered.contains(userLogin)) filtered else Vector(userLogin) ++ filtered
    })

    //    texts.print(10)

    val textsInWindow = texts.window(Minutes(10), Minutes(1))

    textsInWindow.print(10)

    val rules = textsInWindow.transform(rdd => {
      val fpg = new FPGrowth().setMinSupport(0.05)
      val rdd1 = rdd.map(_.toArray)
      val model = fpg.run(rdd1) // Dont allow vector
      model.generateAssociationRules(0.5).sortBy(-_.confidence)
      //        model.freqItemsets
    })

    rules.print(10)

    ssc.start() // Start the computation
    ssc.awaitTermination() // Wait for the computation to terminate


  }

  val stopWordSp = ("a, acuerdo, adelante, ademas, además, adrede, ahi, ahí, ahora, al, alli, allí, alrededor, " +
    "antano, antaño, ante, antes, apenas, aproximadamente, aquel, aquél, aquella, aquélla, aquellas, " +
    "aquéllas, aquello, aquellos, aquéllos, aqui, aquí, arribaabajo, asi, así, aun, aún, aunque, b, bajo, " +
    "bastante, bien, breve, c, casi, cerca, claro, como, cómo, con, conmigo, contigo, contra, cual, cuál, " +
    "cuales, cuáles, cuando, cuándo, cuanta, cuánta, cuantas, cuántas, cuanto, cuánto, cuantos, cuántos, " +
    "d, de, debajo, del, delante, demasiado, dentro, deprisa, desde, despacio, despues, después, detras, " +
    "detrás, dia, día, dias, días, donde, dónde, dos, durante, e, el, él, ella, ellas, ellos, en, encima, " +
    "enfrente, enseguida, entre, es, esa, ésa, esas, ésas, ese, ése, eso, esos, ésos, esta, está, ésta, " +
    "estado, estados, estan, están, estar, estas, éstas, este, éste, esto, estos, éstos, ex, excepto, f, " +
    "final, fue, fuera, fueron, g, general, gran, h, ha, habia, había, habla, hablan, hace, hacia, han, " +
    "hasta, hay, horas, hoy, i, incluso, informo, informó, j, junto, k, l, la, lado, las, le, lejos, lo, " +
    "los, luego, m, mal, mas, más, mayor, me, medio, mejor, menos, menudo, mi, mí, mia, mía, mias, mías, " +
    "mientras, mio, mío, mios, míos, mis, mismo, mucho, muy, n, nada, nadie, ninguna, no, nos, nosotras, " +
    "nosotros, nuestra, nuestras, nuestro, nuestros, nueva, nuevo, nunca, o, os, otra, otros, p, pais, " +
    "paìs, para, parte, pasado, peor, pero, poco, por, porque, pronto, proximo, próximo, puede, q, qeu, " +
    "que, qué, quien, quién, quienes, quiénes, quiza, quizá, quizas, quizás, r, raras, repente, s, salvo, " +
    "se, sé, segun, según, ser, sera, será, si, sí, sido, siempre, sin, sobre, solamente, solo, sólo, son, " +
    "soyos, su, supuesto, sus, suya, suyas, suyo, t, tal, tambien, también, tampoco, tarde, te, temprano, " +
    "ti, tiene, todavia, todavía, todo, todos, tras, tu, tú, tus, tuya, tuyas, tuyo, tuyos, u, un, una, " +
    "unas, uno, unos, usted, ustedes, v, veces, vez, vosotras, vosotros, vuestra, vuestras, vuestro, " +
    "vuestros, w, x, y, ya, yo, z").split(", ")
  val stopWordEn = ("a, a's, able, about, above, according, accordingly, across, actually, after, afterwards," +
    " again, against, ain't, all, allow, allows, almost, alone, along, already, also, although, always," +
    " am, among, amongst, an, and, another, any, anybody, anyhow, anyone, anything, anyway, anyways, " +
    "anywhere, apart, appear, appreciate, appropriate, are, aren't, around, as, aside, ask, asking," +
    " associated, at, available, away, awfully, b, be, became, because, become, becomes, becoming, been," +
    " before, beforehand, behind, being, believe, below, beside, besides, best, better, between, beyond," +
    " both, brief, but, by, c, c'mon, c's, came, can, can't, cannot, cant, cause, causes, certain, " +
    "certainly, changes, clearly, co, com, come, comes, concerning, consequently, consider, considering," +
    " contain, containing, contains, corresponding, could, couldn't, course, currently, d, definitely," +
    " described, despite, did, didn't, different, do, does, doesn't, doing, don't, done, down, downwards," +
    " during, e, each, edu, eg, eight, either, else, elsewhere, enough, entirely, especially, et, etc," +
    " even, ever, every, everybody, everyone, everything, everywhere, ex, exactly, example, except, f," +
    " far, few, fifth, first, five, followed, following, follows, for, former, formerly, forth, four, " +
    "from, further, furthermore, g, get, gets, getting, given, gives, go, goes, going, gone, got, gotten," +
    " greetings, h, had, hadn't, happens, hardly, has, hasn't, have, haven't, having, he, he's, hello, " +
    "help, hence, her, here, here's, hereafter, hereby, herein, hereupon, hers, herself, hi, him, himself," +
    " his, hither, hopefully, how, howbeit, however, i, i'd, i'll, i'm, i've, ie, if, ignored, immediate, " +
    "in, inasmuch, inc, indeed, indicate, indicated, indicates, inner, insofar, instead, into, inward, is," +
    " isn't, it, it'd, it'll, it's, its, itself, j, just, k, keep, keeps, kept, know, knows, known, l, last," +
    " lately, later, latter, latterly, least, less, lest, let, let's, like, liked, likely, little, look," +
    " looking, looks, ltd, m, mainly, many, may, maybe, me, mean, meanwhile, merely, might, more, moreover," +
    " most, mostly, much, must, my, myself, n, name, namely, nd, near, nearly, necessary, need, needs, " +
    "neither, never, nevertheless, new, next, nine, no, nobody, non, none, noone, nor, normally, not," +
    " nothing, novel, now, nowhere, o, obviously, of, off, often, oh, ok, okay, old, on, once, one, ones," +
    " only, onto, or, other, others, otherwise, ought, our, ours, ourselves, out, outside, over, overall," +
    " own, p, particular, particularly, per, perhaps, placed, please, plus, possible, presumably, probably," +
    " provides, q, que, quite, qv, r, rather, rd, re, really, reasonably, regarding, regardless, regards, " +
    "relatively, respectively, right, s, said, same, saw, say, saying, says, second, secondly, see, seeing," +
    " seem, seemed, seeming, seems, seen, self, selves, sensible, sent, serious, seriously, seven, several," +
    " shall, she, should, shouldn't, since, six, so, some, somebody, somehow, someone, something, sometime," +
    " sometimes, somewhat, somewhere, soon, sorry, specified, specify, specifying, still, sub, such, sup," +
    " sure, t, t's, take, taken, tell, tends, th, than, thank, thanks, thanx, that, that's, thats, the," +
    " their, theirs, them, themselves, then, thence, there, there's, thereafter, thereby, therefore," +
    " therein, theres, thereupon, these, they, they'd, they'll, they're, they've, think, third, this," +
    " thorough, thoroughly, those, though, three, through, throughout, thru, thus, to, together, too, took," +
    " toward, towards, tried, tries, truly, try, trying, twice, two, u, un, under, unfortunately, unless," +
    " unlikely, until, unto, up, upon, us, use, used, useful, uses, using, usually, uucp, v, value, various," +
    " very, via, viz, vs, w, want, wants, was, wasn't, way, we, we'd, we'll, we're, we've, welcome, well, " +
    "went, were, weren't, what, what's, whatever, when, whence, whenever, where, where's, whereafter," +
    " whereas, whereby, wherein, whereupon, wherever, whether, which, while, whither, who, who's, whoever," +
    " whole, whom, whose, why, will, willing, wish, with, within, without, won't, wonder, would, would, " +
    "wouldn't, x, y, yes, yet, you, you'd, you'll, you're, you've, your, yours, yourself, yourselves, z, zero").
    split(", ")

  val stopWords = HashSet[String]() + "rt" + "http" + "https" + "noticia" + "noticias" ++ stopWordEn ++ stopWordSp

  val rege = "\\P{L}+"

}
