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

data class Card(val name: String, val imageId: String, val pitchValue: Int?, val setCode: String)

suspend fun main() {
    // Initialize the db connection and the db itself
    Database.connect("jdbc:sqlite:file:cards.db", "org.sqlite.JDBC")
    TransactionManager.manager.defaultIsolationLevel = Connection.TRANSACTION_SERIALIZABLE
    transaction {
        SchemaUtils.createMissingTablesAndColumns(Cards)
    }

    val userDir = FileUtils.getUserDirectory()
    val baseDir = File(userDir, "Downloaded FaB Images").apply { mkdir() }
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

    allCards.forEach { card ->
        val setFolder = File(baseDir, card.setCode).apply { mkdir() }

        val pitch = mapOf(1 to "Red", 2 to "Yellow", 3 to "Blue")
        val nameWithPitch = card.name + (card.pitchValue?.run { " (${pitch[this]})" } ?: "")
        println("${card.setCode}: $nameWithPitch")

        val response = httpClient.get {
            accept(ContentType.Image.PNG)
            url {
                protocol = URLProtocol.HTTPS
                host = "storage.googleapis.com"
                path(
                    "fabmaster", "media", "images", "${card.imageId}.width-450.png"
                )
            }
            retry {
                retryOnExceptionOrServerErrors(maxRetries = 3)
                exponentialDelay()
            }
        }

        File(setFolder, "$nameWithPitch.png").writeBytes(response.body())
    }
}