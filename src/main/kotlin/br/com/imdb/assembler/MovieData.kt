package br.com.imdb.assembler

class MovieData(val title: String, val year: Int) {
    fun toMovie(): Movie {
        return Movie(title, year)
    }
}