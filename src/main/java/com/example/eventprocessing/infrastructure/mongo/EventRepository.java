package com.example.eventprocessing.infrastructure.mongo;

import com.example.eventprocessing.domain.Event;
import com.example.eventprocessing.domain.EventStatus;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface EventRepository extends MongoRepository<Event, String>, EventRepositoryCustom {

    Optional<Event> findByEventId(String eventId);

    long countByStatus(EventStatus status);
}
