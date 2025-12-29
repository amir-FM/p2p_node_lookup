/************************************************************
 * TEMA 2 – Algoritmi Paraleli și Distribuiți
 * Protocolul CHORD în MPI
 *
 * Studenții trebuie să implementeze:
 *   1. build_finger_table() - construirea finger table CHORD
 *   2. closest_preceding_finger() - rutarea logaritmică
 *   3. handle_lookup_request() - logica de lookup distribuit
 *   4. inițierea lookup-urilor locale
 *   5. service-loop-ul distribuit
 *
 * Ce este deja implementat:
 *   - citirea inputului
 *   - formarea inelului CHORD static
 *   - maparea ID -> rank
 *   - structurile de date CHORD
 *   - primitive pentru succesor, predecesor, ID-uri globale
 *
 * Tema se rezolvă STRICT folosind:
 *   - MPI_Send / MPI_Recv
 *   - mesaje cu tag-urile TAG_LOOKUP_REQ, TAG_LOOKUP_REP și TAG_DONE
 ************************************************************/

#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define M 4 // 2^m = 16 ID-uri posibile
#define RING_SIZE 16
#define MAX_NODES 32
#define MAX_PATH 32

/************************************************************
 * Structuri CHORD
 ************************************************************/
typedef struct {
    int start;              // start[i] = (id + 2^i) mod 2^m
    int node;               // finger[i] = succesor(start)
} Finger;

typedef struct {
    int id;                 // ID CHORD al nodului curent
    int successor;          // succesorul în inel
    int predecessor;        // predecesorul în inel
    Finger finger[M];       // finger table-ul (de completat de voi)
} NodeState;

typedef struct {
    int initiator_id;       // cine a inițiat lookup-ul
    int current_id;         // nodul care procesează mesajul
    int key;                // cheia căutată
    int path[MAX_PATH];     // traseul lookup-ului
    int path_len;           // lungimea traseului
} LookupMsg;

/************************************************************
 * Tag-uri mesaje MPI
 ************************************************************/
enum {
    TAG_LOOKUP_REQ = 1,
    TAG_LOOKUP_REP,
    TAG_DONE
};

/************************************************************
 * Variabile globale utile
 ************************************************************/
int world_rank, world_size;
NodeState self;

int all_ids[MAX_NODES];      // ID CHORD pentru fiecare rank
int id_to_rank[RING_SIZE];   // mapare inversă: CHORD ID -> MPI rank
int sorted_ids[MAX_NODES];   // ID-uri sortate ale nodurilor din inel
int num_nodes;

/************************************************************
 * Funcții utile pentru interval circular CHORD
 ************************************************************/
int in_interval(int x, int start, int end) {
    if (start < end)
        return (x > start && x <= end);
    if (start > end)
        return (x > start || x <= end);
    return 1;   // intervalul acoperă tot cercul
}

/************************************************************
 * Construirea mapării rank -> id și id -> rank
 ************************************************************/
void build_id_maps() {
    for (int i = 0; i < RING_SIZE; i++)
        id_to_rank[i] = -1;

    for (int r = 0; r < world_size; r++) {
        int id = all_ids[r];
        if (id >= 0 && id < RING_SIZE)
            id_to_rank[id] = r;
    }
}

int rank_from_id(int id) {
    if (id < 0 || id >= RING_SIZE)
        return -1;
    return id_to_rank[id];
}

/************************************************************
 * Construirea inelului CHORD static
 ************************************************************/
int cmp_int(const void *a, const void *b) {
    return (*(int*)a - *(int*)b);
}

void build_global_ring() {
    num_nodes = world_size;

    for (int i = 0; i < num_nodes; i++)
        sorted_ids[i] = all_ids[i];

    qsort(sorted_ids, num_nodes, sizeof(int), cmp_int);

    for (int i = 0; i < num_nodes; i++) {
        int id = sorted_ids[i];
        int succ = sorted_ids[(i + 1) % num_nodes];
        int pred = sorted_ids[(i - 1 + num_nodes) % num_nodes];

        if (id == self.id) {
            self.successor = succ;
            self.predecessor = pred;
        }
    }
}

/************************************************************
 * Construirea finger table-ului – de completat de voi
 *
 * IMPORTANT:
 *  Tema folosește o variantă STATICĂ a CHORD, iar succesorul unei
 *  poziții "start" se caută EXCLUSIV în lista nodurilor existente,
 *  nu în întreg spațiul 0..RING_SIZE-1 (așa cum ar fi în CHORD real).
 *
 *  find_successor_simple() caută succesorul lui start[i] în sorted_ids[],
 *  care conține DOAR nodurile existente.
 ************************************************************/
int find_successor_simple(int key) {
    for (int i = 0; i < num_nodes; i++)
        if (sorted_ids[i] >= key)
            return sorted_ids[i];
    return sorted_ids[0]; // wrap-around
}

void build_finger_table() {

    /******************************************************
     * TODO 1 – de implementat de voi
     *
     * Atenție:
     *   - succesorul se caută doar în lista sorted_ids
     *   - inelul este static, deci finger table-ul este static
     *   - este parte OBLIGATORIE a temei
     ******************************************************/

}

/************************************************************
 * closest_preceding_finger – de implementat de voi
 *
 *  Această funcție returnează cel mai "mare" finger care
 *  se află strict în intervalul CHORD (self.id, key).
 *
 * Atenție:
 *    – Intervalul este unul circular și DESCHIS la self.id.
 *    – Finger-ul ales trebuie să ducă lookup-ul mai aproape
 *      de key în mod real.
 *    – Alegerea unui finger care nu produce progres (de ex.
 *      care se întoarce la self.id) poate duce la cicluri
 *      infinite și blocarea rutării.
 *
 *  Dacă niciun finger nu se potrivește, întoarceți succesorul.
 ************************************************************/
int closest_preceding_finger(int key) {

    /******************************************************
     * TODO 2 - de implementat de voi
     ******************************************************/

    return self.successor;   // fallback
}

/************************************************************
 * handle_lookup_request – de implementat de voi
 *
 *   Aceasta este funcția esențială pentru rutare distribuită.
 *   Pașii corecți CHORD (tema simplificată):
 *
 *   1. Adăugați self.id în path.
 *   2. Dacă succesorul nostru este responsabil de key:
 *           - adăugați succesorul în path
 *           - trimiteți TAG_LOOKUP_REP către inițiator
 *   3. Altfel:
 *           - next = closest_preceding_finger(key)
 *           - trimiteți TAG_LOOKUP_REQ către closest_preceding_finger
 *
 ************************************************************/
void handle_lookup_request(LookupMsg *msg) {
    /******************************************************
     * TODO 3 – de implementat de voi
     ******************************************************/
}

int main(int argc, char **argv) {

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    /******************************************************
     * Citire input
     ******************************************************/
    char fname[32];
    sprintf(fname, "in%d.txt", world_rank);
    FILE *f = fopen(fname, "r");
    if (!f) {
        fprintf(stderr, "Rank %d: cannot open %s\n", world_rank, fname);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    int nr_lookups;
    fscanf(f, "%d", &self.id);
    fscanf(f, "%d", &nr_lookups);

    int *lookups = malloc(nr_lookups * sizeof(int));
    for (int i = 0; i < nr_lookups; i++)
        fscanf(f, "%d", &lookups[i]);
    fclose(f);

    /******************************************************
     * Distribuirea ID-urilor tuturor nodurilor
     ******************************************************/
    MPI_Allgather(&self.id, 1, MPI_INT,
                  all_ids, 1, MPI_INT,
                  MPI_COMM_WORLD);

    build_id_maps();
    build_global_ring();
    build_finger_table();

    MPI_Barrier(MPI_COMM_WORLD);

    /************************************************************
     * TODO 4 - inițierea lookup-urilor locale
     *
     *    Pentru fiecare valoare Key citită din input:
     *       - construiți LookupMsg
     *       - trimiteți un mesaj de tip TAG_LOOKUP_REQ către propriul rank
     ************************************************************/

    free(lookups);

    /************************************************************
     * TODO 5 - service-loop distribuit
     *
     *    Scop:
     *       - primiți mesaje cu MPI_Recv()
     *       - procesați TAG_LOOKUP_REQ → handle_lookup_request()
     *       - procesați TAG_LOOKUP_REP → afișare + countdown local
     *       - trimiteți TAG_DONE tuturor când ați terminat
     *       - opriți loop-ul doar când primiți DONE de la toate nodurile
     ************************************************************/

    MPI_Finalize();
    return 0;
}
