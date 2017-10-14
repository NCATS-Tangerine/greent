path_to(X,Y) :-
    translates(X,Y,M),
    notify([M]).
path_to(X,Y) :-
    translates(X,Z,M),
    translates(Z,Y,N),
    notify([M,N]).

transitions(X,Y):-
    findall(Z, (member(A, path_to(X,Y))), Ts).
