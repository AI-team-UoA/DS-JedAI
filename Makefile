dockerize:
	docker build -t git.project-hobbit.eu:4567/gmandi/dsjedai .

	docker login git.project-hobbit.eu:4567

	docker push git.project-hobbit.eu:4567/gmandi/dsjedai