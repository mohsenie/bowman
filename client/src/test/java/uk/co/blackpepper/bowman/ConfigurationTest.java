package uk.co.blackpepper.bowman;

import java.net.URI;
import java.util.Optional;

import org.junit.Test;

import uk.co.blackpepper.bowman.annotation.RemoteResource;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class ConfigurationTest {
	
	@RemoteResource("/y")
	private static class Entity implements ClientFactoryCallBackInterface{

		@Override
		public void setPagination(Pagination pagination) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public Optional<Pagination> getPagination() {
			// TODO Auto-generated method stub
			return null;
		}
	}
	
	@Test
	public void buildClientFactoryBuildsFactoryWithConfiguration() {
		ClientFactory factory = Configuration.builder()
			.setBaseUri(URI.create("http://x.com")).build().buildClientFactory();
		
		Client<Entity> client = factory.create(Entity.class, null);
		
		assertThat(client.getBaseUri(), is(URI.create("http://x.com/y")));
	}
}
