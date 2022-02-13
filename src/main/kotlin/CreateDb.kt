import it.skrape.core.htmlDocument
import it.skrape.fetcher.HttpFetcher
import it.skrape.fetcher.response
import it.skrape.fetcher.skrape
import it.skrape.selects.DocElement
import java.time.LocalDateTime

val allSets = listOf("welcome-rathe", "arcane-rising", "crucible-war", "monarch", "tales-aria", "everfest")
const val gallery_url_template = "https://fabtcg.com/resources/card-galleries/%s-booster/"

fun main(args: Array<String>) {
    val parentDivs = skrape(HttpFetcher) {
        request {
            url = gallery_url_template
        }.also { println("call ${it.preparedRequest.url} at ${LocalDateTime.now()}") }
        response {
            htmlDocument {
                findAll("div.listblock-item")
            }
        }
    }
    val maxLen = parentDivs.flatMap { it.findAll(cssSelector = "div.card-details > h5") }
        .maxOf { it.text.length }
    println(parentDivs.map { it.findFirst("div.card-details > h5").text }
        .filter { it.matches(Regex(".+?\\([123]\\)")) }
        .groupBy { Regex("^([^(]+) ").find(it)?.groups?.get(1)?.value }
        .filter { it.value.size != 3 })
}

fun printStats(e: DocElement, padding: Int) {
    val fmtStr = "%${padding}s %s"
    println(
        String.format(
            fmtStr,
            e.findFirst("div.card-details > h5").text,
            e.findFirst("a > img").attribute("src")
        )
    )
}