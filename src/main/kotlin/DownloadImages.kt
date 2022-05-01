import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.engine.cio.*
import io.ktor.client.plugins.*
import io.ktor.client.request.*
import io.ktor.http.*
import org.apache.commons.io.FileUtils
import org.jetbrains.exposed.sql.Database
import org.jetbrains.exposed.sql.SchemaUtils
import org.jetbrains.exposed.sql.selectAll
import org.jetbrains.exposed.sql.transactions.TransactionManager
import org.jetbrains.exposed.sql.transactions.transaction
import java.io.File
import java.sql.Connection

// Simple wrapper for a card in the db, used to avoid a conflict between CIO and transaction { }
data class Card(val name: String, val imageId: String, val pitchValue: Int?, val setCode: String)

val pitchValToRYB = mapOf(1 to "Red", 2 to "Yellow", 3 to "Blue")

suspend fun main() {
    // Initialize the db connection and the db itself
    Database.connect("jdbc:sqlite:file:cards.db", "org.sqlite.JDBC")
    TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
    transaction {
        SchemaUtils.createMissingTablesAndColumns(Cards)
    }

    // Initialize the output dir to be written to, create an HttpClient, and get a list of all cards
    val baseDir = File(FileUtils.getUserDirectory(), "Downloaded FaB Images").apply { mkdir() }
    val httpClient = HttpClient(CIO)
    val allCards = transaction {
        Cards.selectAll().map {
            Card(
                it[Cards.name],
                it[Cards.imageId],
                it[Cards.pitchValue],
                it[Cards.setCode]
            )
        }
    }

    // Download the image of each card and store it in the folder for its set
    allCards.forEach { card ->
        val setFolder = File(baseDir, card.setCode).apply { mkdir() }

        val nameWithPitch = card.name + (card.pitchValue?.run { " (${pitchValToRYB[this]})" } ?: "")
        println("Fetching ${card.setCode}: $nameWithPitch")

        // Download the image
        val response = httpClient.get {
            accept(ContentType.Image.PNG)
            url(getUrlForImageId(card.imageId))
            retry {
                retryOnExceptionOrServerErrors()
                exponentialDelay()
            }
        }

        // Save the image
        File(setFolder, "$nameWithPitch.png").writeBytes(response.body())
    }
}

fun getUrlForImageId(imageId: String) = URLBuilder() .apply {
    protocol = URLProtocol.HTTPS
    host = "storage.googleapis.com"
    path(
        "fabmaster", "media", "images", "$imageId.width-450.png"
    )
}.build()