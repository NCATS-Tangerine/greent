path_to(X,Y) :-
    translates(X,Y,M).
path_to(X,Y) :-
    is_a(X,A),
    has_context(A,T),
    path_to(A,Y).
path_to(X,Y) :-
    translates(X,Z,M),
    translates(Z,Y,N).

transitions(X,Y,Ts):-
    findall(Z, (member(A, path_to(X,Y))), Ts).
