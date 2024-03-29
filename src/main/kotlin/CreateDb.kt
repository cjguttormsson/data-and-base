import it.skrape.core.htmlDocument
import it.skrape.fetcher.HttpFetcher
import it.skrape.fetcher.response
import it.skrape.fetcher.skrape
import it.skrape.selects.DocElement
import it.skrape.selects.ElementNotFoundException
import org.jetbrains.exposed.exceptions.ExposedSQLException
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.Table
import org.jetbrains.exposed.sql.insert
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import org.sqlite.SQLiteException
import java.sql.Connection
import java.time.LocalDateTime
import java.util.concurrent.TimeUnit

// links to the pages of all unique card sets
val galleryUrls = listOf(
    "https://fabtcg.com/resources/card-galleries/welcome-rathe-booster/",
    "https://fabtcg.com/resources/card-galleries/arcane-rising/",
    "https://fabtcg.com/resources/card-galleries/crucible-war-booster/",
    "https://fabtcg.com/resources/card-galleries/monarch-booster/",
    "https://fabtcg.com/resources/card-galleries/tales-aria-booster/",
    "https://fabtcg.com/resources/card-galleries/everfest-booster/",
    "https://fabtcg.com/resources/card-galleries/uprising/",

    "https://fabtcg.com/resources/card-galleries/welcome-deck-2019/",

    "https://fabtcg.com/resources/card-galleries/bravo-hero-deck/",
    "https://fabtcg.com/resources/card-galleries/dorinthea-hero-deck/",
    "https://fabtcg.com/resources/card-galleries/katsu-hero-deck/",
    "https://fabtcg.com/resources/card-galleries/rhinar-hero-deck/",

    "https://fabtcg.com/products/booster-set/monarch/monarch-blitz-decks/boltyn-blitz-deck/",
    "https://fabtcg.com/products/booster-set/monarch/monarch-blitz-decks/chane-blitz-deck/",
    "https://fabtcg.com/products/booster-set/monarch/monarch-blitz-decks/levia-blitz-deck/",
    "https://fabtcg.com/products/booster-set/monarch/monarch-blitz-decks/prism-blitz-deck/",

    "https://fabtcg.com/resources/card-galleries/tales-aria/briar-blitz-deck/",
    "https://fabtcg.com/resources/card-galleries/tales-aria/lexi-blitz-deck/",
    "https://fabtcg.com/resources/card-galleries/tales-aria/oldhim-blitz-deck/",
)

// Some cards don't follow the convention for image names, this map fixes them
val replacementCardIds = mapOf(
    "16984263482378r4623792" to "MON011",
    "9986165132..223435430" to "MON017",
    "23fgw5465b464" to "MON018",
    "456b443654yteb65764" to "MON019",
    "fy8w7r78545yit3787efygs8def" to "MON091",
    "Mty2ZQtPLgqDHz9EjdOMHZOUJhISA3s8RIgr3lus4KTmVq" to "MON119",
    "QB393QB0F93d2Hhhs5Cf_iMdCfa8qpl1J91FOlwlsYvSnv" to "MON120",
    "KLUiCIJEFKdt8QM545A4g" to "MON125",
    "8Hvi6X0i746eD8UVlBjt" to "MON126",
    "3HnONyYapgg32MEXbFXUB" to "MON127",
    "SAy5p6Yoa21bM89UuG8l4" to "MON128",
    "83qQVRV7av7WVwN6jhg0d" to "MON223",
    "322d1Gx66IHv4QNM3gOjV7" to "MON224",
    "h3ntlAv43eGM6Nq3R046zh" to "MON225",
    "BRi0111" to "BRI011",
    "BVO" to "BVO001",
    "TEA" to "TEA001",
    "KSU" to "KSU001",
    "RNR" to "RNR001",
    "ICE_CM1lpsjE9_01" to "UPR102",
    "ICE_CM1lpsjE9_02" to "UPR103",
    "ICE_CM1lpsjE9_03" to "UPR104",
    "ICE_CM1lpsjE9_04" to "UPR135"
)

val CARD_URL_UNKNOWN = "https://storage.googleapis.com/fabmaster/media/images/darkness.width-450.png"

// Regexes for parsing the image URLs and names for cards
val imageIdPattern = Regex("https://storage\\.googleapis\\.com/fabmaster/media/images/(.+?)\\.width-\\d+\\.png")
val cardIdPattern = Regex("^([A-Z]{3})[-_]?(\\d{1,3})")
val cardNamePattern = Regex("^([^(]+)( \\(([123])\\))?$")

// Schema for the table. Example: (set_code=EVR, set_index=50, name=Wax On, pitch_value=1)
object Cards : Table() {
    val setCode = char("set_code", 3)
    val setIndex = integer("set_index").check("CHECK_SET_INDEX") { it.greaterEq(0) }
    val name = text("name").index("INDEX_NAME") // with pitch value suffix (eg. "(3)") removed
    val pitchValue = integer("pitch_value").check("CHECK_PITCH") { it.between(1, 3) }.nullable()
    val imageId = text("image_id")

    override val primaryKey = PrimaryKey(imageId)
}

fun main() {
    // Initialize the db connection and the db itself
    Database.connect("jdbc:sqlite:file:cards.db", "org.sqlite.JDBC")
    TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
    transaction {
        SchemaUtils.createMissingTablesAndColumns(Cards)
    }

    for (galleryUrl: String in galleryUrls) {
        // Get the page for one set and collect all the divs with cards
        val elements = skrape(HttpFetcher) {
            request {
                url = galleryUrl
                timeout = 15_000 // ms
            }.also { println("scraping ${it.preparedRequest.url} at ${LocalDateTime.now()}") }
            response {
                htmlDocument {
                    findAll("div.listblock-item")
                }
            }
        }

        var failedToScrape = 0
        var cardWasUnknown = 0
        var failedToParse = 0
        var failedToInsert = 0

        for (element: DocElement in elements) {
            // Extract the needed values for one card
            val nameAndPitch: String
            val cardUrl: String
            try {
                nameAndPitch = element.findFirst("div.card-details > h5").text
                cardUrl = element.findFirst("a > img").attribute("src")
            } catch (e: ElementNotFoundException) {
                println("Couldn't scrape listblock-item ${element.text}")
                failedToScrape++
                continue
            }

            // Skip cards marked "Unknown" (ie. not yet announced)
            if (cardUrl == CARD_URL_UNKNOWN) {
                cardWasUnknown++
                continue
            }

            // Try to extract the details
            var cardImageId: String
            var cardSetCode: String
            var cardSetIndex: String
            var cardName: String
            var cardPitchValue: String?
            try {
                cardImageId = imageIdPattern.find(cardUrl)!!.groupValues[1]
                cardSetCode = cardIdPattern.find(
                    replacementCardIds.getOrDefault(cardImageId, cardImageId)
                )!!.groupValues[1]
                cardSetIndex = cardIdPattern.find(
                    replacementCardIds.getOrDefault(cardImageId, cardImageId)
                )!!.groupValues[2]
                cardName = cardNamePattern.find(nameAndPitch)!!.groupValues[1]
                // groupValues would give an empty string here, but we want null
                cardPitchValue = cardNamePattern.find(nameAndPitch)?.groups?.get(3)?.value
            } catch (e: NullPointerException) {
                println("Couldn't create a database entry!\nname: $nameAndPitch\nurl: $cardUrl\n")
                failedToParse++
                continue
            }


            try {
                // Try to insert the card into the db
                transaction {
                    try {
                        Cards.insert {
                            it[setCode] = cardSetCode
                            it[setIndex] = cardSetIndex.toInt()
                            it[name] = cardName
                            it[pitchValue] = cardPitchValue?.toInt()
                            it[imageId] = cardImageId
                        }
                    } catch (e: ExposedSQLException) {
                        e.cause?.let { throw it }
                    }
                }
            } catch (e: SQLiteException) {
                failedToInsert++
                println("Couldn't insert into the db!\nname: $nameAndPitch\ncode: ${e.resultCode}")
            }
        }

        // Sleep for a bit to avoid hitting any rate limits
        val t: Long = 2 // seconds
        val urlSegmentName = galleryUrl.split("/").last { it.isNotEmpty() }
        println("Processed ${elements.size} cards from $urlSegmentName, sleeping for $t seconds...")
        print("$failedToScrape card(s) failed to scrape, $cardWasUnknown unknown card(s), ")
        println("$failedToParse card(s) failed to parse, and $failedToInsert card(s) failed to insert")
        TimeUnit.SECONDS.sleep(t)
    }
}