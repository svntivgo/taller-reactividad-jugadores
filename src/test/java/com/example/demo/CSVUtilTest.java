package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {

    @Test
    void converterData(){
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void stream_filtrarJugadoresPorId(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.id == 158023)
                .distinct()
                .collectMultimap(Player::getClub);


        listFilter.block().forEach((equipo, players) ->
        {
            System.out.println("Equipo: " + equipo);
            players.forEach(player ->
            {
                System.out.println("Jugador: " + player.name + ", Edad: " +player.age + ", Nacionalidad: " +player.national );
                assert player.id == 158023;
            });
        } );
    }

    @Test
    void stream_ordenarJugadoresPorEdad(){

    }

    @Test
    void stream_filtrarJugadoresMayoresA34(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age>34 && player.club.equals("Juventus"))
                .distinct()
                .collectMultimap(Player::getClub);


        listFilter.block().forEach((equipo, players) ->
        {
            System.out.println("Equipo: " + equipo);
            players.forEach(player ->
            {
                System.out.println("Jugador: " + player.name + ", Edad: " +player.age );
                assert player.club.equals("Juventus");
                assert player.age >34;
            });
        } );
    }

    @Test
    void stream_filtrarJugadoresPorEquipo(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.club.equals("Juventus"))
                .distinct()
                .collectMultimap(Player::getClub);


        listFilter.block().forEach((equipo, players) ->
        {
            System.out.println("Equipo: " + equipo);
            players.forEach(player ->
            {
                System.out.println("Jugador: " + player.name);
                assert player.club.equals("Juventus");
            });
        } );
    }

    @Test
    void stream_filtrarJugadoresPorNacionalidad(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.national.equals("France"))
                .distinct()
                .collectMultimap(Player::getNational);


        listFilter.block().forEach((nacionalidad, players) ->
        {
            System.out.println("Nacionalidad: " +nacionalidad);
            players.forEach(player ->
            {
                System.out.println("Jugador: " + player.name);
                assert player.national.equals("France");
            });
        } );
    }

    @Test
    void stream_filtrarNacionalidades(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .distinct()
                .collectMultimap(Player::getNational);

        listFilter.block().forEach((nacionalidad, players) ->
        {
            System.out.println("Nacionalidad: " +nacionalidad);
        } );
    }

    @Test
    void stream_filtrarRankingJugadoresPorNacionalidad(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .sort(Comparator.comparingInt(Player::getWinners).reversed())
                .distinct()
                .collectMultimap(Player::getNational);


        listFilter.block().forEach((nacionalidad, players) ->
        {
            System.out.println("Nacionalidad: " +nacionalidad);
            players.forEach(player ->
            {
                System.out.println("Jugador: " + player.name + ", wins: " + player.winners);
            });
        } );
    }



}
