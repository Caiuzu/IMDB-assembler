package br.com.imdb.assembler

class MovieApplication {
    fun run() {
        val movieProcessor = MovieProcessor()
        val files = movieProcessor.processMovies()
        // TODO: ...
    }
}
