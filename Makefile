init:
	@docker swarm init

deploy:
	@docker stack deploy -c stack.yml jupyter

rm:
	@docker stack rm jupyter

stack:
	@docker stack deploy -c stack.yml jupyter

leave:
	@docker swarm leave --force