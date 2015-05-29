package org.springframework.cloud.zookeeper.discovery;

import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.PreDestroy;

import lombok.SneakyThrows;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.discovery.event.InstanceRegisteredEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.ApplicationEventPublisherAware;
import org.springframework.context.ApplicationListener;

/**
 * @author Spencer Gibb
 */
public class ZookeeperServiceWatch implements
		ApplicationListener<InstanceRegisteredEvent>, PathChildrenCacheListener,
		ApplicationEventPublisherAware {

	private final CuratorFramework curator;
	private final ZookeeperDiscoveryProperties properties;
	private final AtomicLong cacheChange = new AtomicLong(0);
	private ApplicationEventPublisher publisher;
	private PathChildrenCache cache;

	public ZookeeperServiceWatch(CuratorFramework curator,
			ZookeeperDiscoveryProperties properties) {
		this.curator = curator;
		this.properties = properties;
	}

	@Override
	public void setApplicationEventPublisher(ApplicationEventPublisher publisher) {
		this.publisher = publisher;
	}

	@Override
	@SneakyThrows
	public void onApplicationEvent(InstanceRegisteredEvent event) {
		cache = new PathChildrenCache(curator, properties.getRoot(), false);
		cache.getListenable().addListener(this);
		cache.start();
	}

	@PreDestroy
	public void stop() throws Exception {
		if (cache != null) {
			cache.close();
		}
	}

	@Override
	public void childEvent(CuratorFramework client, PathChildrenCacheEvent event)
			throws Exception {
		long newCacheChange = cacheChange.incrementAndGet();
		publisher.publishEvent(new HeartbeatEvent(this, newCacheChange));
	}
}
