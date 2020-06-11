package uk.co.blackpepper.bowman.test.it;

import java.net.URI;
import java.util.Optional;

import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import uk.co.blackpepper.bowman.Client;
import uk.co.blackpepper.bowman.ClientFactoryCallBackInterface;
import uk.co.blackpepper.bowman.Pagination;
import uk.co.blackpepper.bowman.test.it.model.NullLinkedCollectionEntity;
import uk.co.blackpepper.bowman.test.it.model.SimpleEntity;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class NullLinkedCollectionIT extends AbstractIT {
	
	private Client<NullLinkedCollectionEntity> client;

	private Client<SimpleEntity> simpleEntityClient;
	
	@Before
	public void setUp() {
		client = clientFactory.create(NullLinkedCollectionEntity.class, new ClientFactoryCallBackInterface() {
			@Override
			public void setPagination(Pagination pagination) {

			}

			@Override
			public Optional<Pagination> getPagination() {
				return Optional.empty();
			}
		});
		simpleEntityClient = clientFactory.create(SimpleEntity.class, new ClientFactoryCallBackInterface() {
			@Override
			public void setPagination(Pagination pagination) {

			}

			@Override
			public Optional<Pagination> getPagination() {
				return Optional.empty();
			}
		});
	}
	
	@Test
	public void canGetInitiallyNullLinkedCollection() {
		SimpleEntity linked = new SimpleEntity();
		URI linkedLocation = simpleEntityClient.post(linked);
		
		NullLinkedCollectionEntity entity = new NullLinkedCollectionEntity();
		entity.setLinked(Sets.newHashSet(linked));
		client.post(entity);
		
		NullLinkedCollectionEntity retrieved = client.get(entity.getId());
		
		assertThat(retrieved.getLinked(), contains(hasProperty("id", is(linkedLocation))));
	}
}
