#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <time.h>
#include <mpi.h>
#include <math.h>
#include <stdio.h>

using std::vector;
using std::string;
using std::cout;
using std::cin;
using std::ofstream;
using std::ifstream;

typedef vector<vector<bool> > Cells;

const int MPI_TAG = 0;

bool GetRandomBool() {
    return rand() > RAND_MAX / 2;
}

void WriteToFile(const Cells& cells, string path) {
    ofstream file(path.c_str(), std::ios::trunc);
    for (int rowIndex = 0; rowIndex < cells.size(); ++rowIndex) {
        for (int columnIndex = 0; columnIndex < cells[rowIndex].size(); columnIndex++) {
            if (cells[rowIndex][columnIndex]) {
                file << "# ";
            } else {
                file << "~ ";
            }
        }
        file << "\n";
    }
    file.close();
}

void WriteToConsole(const Cells& cells) {
    for (int rowIndex = 0; rowIndex < cells.size(); ++rowIndex) {
        for (int columnIndex = 0; columnIndex < cells[rowIndex].size(); columnIndex++) {
            if (cells[rowIndex][columnIndex]) {
                cout << "# ";
            } else {
                cout << "~ ";
            }
        }
        cout << "\n";
    }
}

Cells ReadCellsFromFile(string path) {
    Cells cells;
    ifstream file(path.c_str());
    string line;
    while (file) {
        std::getline(file, line);
        vector<bool> cellsRow;
        for (int i = 0; i < line.size(); i += 2) {
            if (line[i] == '~') {
                cellsRow.push_back(false);
            } else if (line[i] == '#') {
                cellsRow.push_back(true);
            }
        }
        if (!cellsRow.empty()) {
            cells.push_back(cellsRow);
        }
    }
    file.close();
    return cells;
}

Cells CreateEmptyCells(int rowCount, int columnCount) {
    Cells cells(rowCount, vector<bool>(columnCount, false));
    return cells;
}

Cells CreateRandomCells(int rowCount, int columnCount) {
    Cells cells;
    for (int i = 0; i < rowCount; i++) {
        vector<bool> row;
        for (int j = 0; j < columnCount; j++) {
            row.push_back(GetRandomBool());
        }
        cells.push_back(row);
    }
    return cells;
}

int modulus(int i, int mod) {
    if (i < 0) {
        return modulus(i + mod, mod);
    }
    return i % mod;
}

int GetNumberOfNeighbours(const Cells& cells, int rowIndex, int columnIndex) {
    int numberOfNeighbours = 0;
    int numberOfRows = cells.size();
    int numberOfColumns = cells[0].size();
    int i;
    int j;
    /*
     ---
     -0-
     ###
     */
    i = modulus(rowIndex - 1, numberOfRows);
    j = modulus(columnIndex - 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex - 1, numberOfRows);
    j = modulus(columnIndex, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex - 1, numberOfRows);
    j = modulus(columnIndex + 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;
    /*
     ---
     #0#
     ---
     */
    i = modulus(rowIndex, numberOfRows);
    j = modulus(columnIndex - 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex, numberOfRows);
    j = modulus(columnIndex + 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;
    /*
     ###
     -0-
     ---
     */
    i = modulus(rowIndex + 1, numberOfRows);
    j = modulus(columnIndex - 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex + 1, numberOfRows);
    j = modulus(columnIndex, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;

    i = modulus(rowIndex + 1, numberOfRows);
    j = modulus(columnIndex + 1, numberOfColumns);
    numberOfNeighbours += cells[i][j] ? 1 : 0;
    return numberOfNeighbours;
}

void GetBorderFromOtherNodes(Cells* cells, int numberOfNodes, int rank) {
    MPI_Status status;
    int rowNum = cells->size();
    int colNum = cells->at(0).size();
    
    int upperNodeRank = modulus(rank + 1, numberOfNodes);
    int upperBorder[colNum];
    MPI_Recv(upperBorder, colNum, MPI_INT, upperNodeRank, MPI_TAG,
            MPI_COMM_WORLD, &status);

    int lowerNodeRank = modulus(rank - 1, numberOfNodes);
    int lowerBorder[colNum];
    MPI_Recv(lowerBorder, colNum, MPI_INT, lowerNodeRank, MPI_TAG,
            MPI_COMM_WORLD, &status);

    for (int i = 0; i < colNum; i++) {
        cells->at(rowNum - 1).at(i) = upperBorder[i];
    }
    for (int i = 0; i < colNum; i++) {
        cells->at(0).at(i) = lowerBorder[i];
    }
}

void SendBorderToOtherNodes(Cells* cells, int numberOfNodes, int rank) {
    int rowNum = cells->size();
    int colNum = cells->at(0).size();
    int upperBorderBuff[colNum];
    int lowerBorderBuff[colNum];
    for (int i = 0; i < colNum; i++) {
        upperBorderBuff[i] = cells->at(rowNum - 2)[i];
        lowerBorderBuff[i] = cells->at(1)[i];
    }

    int upperNodeRank = modulus(rank + 1, numberOfNodes);
    MPI_Bsend(upperBorderBuff, colNum, MPI_INT,
            upperNodeRank, MPI_TAG, MPI_COMM_WORLD);
    
    int lowerNodeRank = modulus(rank - 1, numberOfNodes);
    MPI_Bsend(lowerBorderBuff, colNum, MPI_INT,
            lowerNodeRank, MPI_TAG, MPI_COMM_WORLD);
}

void CalculateRow(Cells* source, Cells* destitation, int rowIndex) {
    for (int columnIndex = 0; columnIndex < source->at(0).size(); columnIndex++) {
        int numberOfNeighbours = GetNumberOfNeighbours(*source, rowIndex, columnIndex);
        bool newValue;
        if (source->at(rowIndex).at(columnIndex)) {
            if (numberOfNeighbours < 2 || numberOfNeighbours > 3) {
                newValue = false;
            } else {
                newValue = true;
            }
        } else {
            if (numberOfNeighbours == 3) {
                newValue = true;
            } else {
                newValue = false;
            }
        }
        destitation->at(rowIndex).at(columnIndex) = newValue;
    }
}

void DoOneLifeTimeStep(Cells* source, Cells* destitation, int numberOfNodes, int rank) {
    GetBorderFromOtherNodes(source, numberOfNodes, rank);
    int rowNum = source->size();
    CalculateRow(source, destitation, 1);
    CalculateRow(source, destitation, rowNum - 2);
    SendBorderToOtherNodes(destitation, numberOfNodes, rank);
    for (int rowIndex = 2; rowIndex < rowNum - 2; rowIndex++) {
        CalculateRow(source, destitation, rowIndex);
    }
}

void WriteNumberOfNeighboursToConsole(const Cells& cells) {
    for (int rowIndex = 0; rowIndex < cells.size(); ++rowIndex) {
        for (int columnIndex = 0; columnIndex < cells[rowIndex].size(); columnIndex++) {
            cout << GetNumberOfNeighbours(cells, rowIndex, columnIndex) << " ";
        }
        cout << "\n";
    }
}

void DoManyLifeSteps(Cells* source, int n, int numberOfNodes, int rank) {
    Cells tempCells(*source);
    Cells* a = source;
    Cells* b = &tempCells;
    SendBorderToOtherNodes(source, numberOfNodes, rank);
    for (int i = 0; i < n; i++) {
        Cells* t;
        DoOneLifeTimeStep(a, b, numberOfNodes, rank);
        t = b;
        b = a;
        a = t;
    }
    if (n % 2 == 1) {
        source-> clear();
        std::copy(tempCells.begin(), tempCells.end(), std::back_inserter(*source));
    }
}

int* CellsToArray(const Cells& cells) {
    int rowNum = cells.size();
    int colNum = cells[0].size();
    int* array = new int[rowNum * colNum];
    for (int i = 0; i < rowNum; i++) {
        for (int j = 0; j < colNum; j++) {
            array[i * colNum + j] = cells[i][j] ? 1 : 0;
        }
    }
    return array;
}

Cells ArrayToCells(int* array, int rowNum, int colNum) {
    Cells cells = CreateEmptyCells(rowNum, colNum);
    for (int rowIndex = 0; rowIndex < rowNum; ++rowIndex) {
        for (int colIndex = 0; colIndex < colNum; ++colIndex) {
            bool isAlive = (array[rowIndex * colNum + colIndex] == 1);
            cells[rowIndex][colIndex] = isAlive;
        }
    }
    return cells;
}

void SetSendSizesAndDispls(int numberOfNodes,
        int rowNum,
        int colNum,
        int* sendSizes,
        int* displs) {

    double rowNumStep = static_cast<double> (rowNum) / numberOfNodes;
    double currentRow = 0;
    for (int i = 0; i < numberOfNodes; i++) {
        displs[i] = static_cast<int> (currentRow) * colNum;
        currentRow += rowNumStep;
    }
    for (int i = 0; i < numberOfNodes - 1; i++) {
        sendSizes[i] = displs[i + 1] - displs[i];
    }
    sendSizes[numberOfNodes - 1] = rowNum * colNum - displs[numberOfNodes - 1];
}

Cells ScatterNodeCells(int* arrayOfAllCells,
        int* sendSizes,
        int* displs,
        int colNum,
        int rank) {
    int arrayOfNodeCells[sendSizes[rank]];
    MPI_Scatterv(arrayOfAllCells, sendSizes, displs, MPI_INT,
            arrayOfNodeCells, sendSizes[rank], MPI_INT,
            0, MPI_COMM_WORLD);
    Cells cellls = ArrayToCells(arrayOfNodeCells, sendSizes[rank] / colNum, colNum);
    return cellls;
}

Cells CreateTriangularCells(int rowCount, int columnCount) {
    Cells cells = CreateEmptyCells(rowCount, columnCount);
    for (int i = 0; i < rowCount; i++) {
        for (int j = 0; j < columnCount && j < i; j++) {
            cells[i][j] = true;
        }
    }
    return cells;
}

Cells ScatterCellsAcrossNodes(const Cells& cells, int rowNum, int colNum, int numberOfNodes, int rank) {
    int* arrayOfAllCells;
    int sendSizes[numberOfNodes];
    int displs[numberOfNodes];
    if (rank == 0) {
        arrayOfAllCells = CellsToArray(cells);
    }
    SetSendSizesAndDispls(numberOfNodes, rowNum, colNum, sendSizes, displs);
    Cells nodeCells = ScatterNodeCells(arrayOfAllCells,
            sendSizes,
            displs,
            colNum,
            rank);
    if (rank == 0) {
        delete[] arrayOfAllCells;
    }
    return nodeCells;
}

Cells GatherCellsFromAllNodes(const Cells& cells, int rowNum, int colNum, int numberOfNodes, int rank) {
    int nodeRowNum = cells.size();
    int nodeColNum = cells[0].size();
    int* sendArray = CellsToArray(cells);
    int sendSizes[numberOfNodes];
    int displs[numberOfNodes];
    int* allCellsArray;
    if (rank == 0) {
        allCellsArray = new int[rowNum * colNum];
    }
    SetSendSizesAndDispls(numberOfNodes, rowNum, colNum, sendSizes, displs);
    MPI_Gatherv(sendArray, nodeRowNum* nodeColNum, MPI_INT,
            allCellsArray, sendSizes, displs,
            MPI_INT, 0, MPI_COMM_WORLD);
    delete [] sendArray;
    if (rank == 0) {
        Cells cells = ArrayToCells(allCellsArray, rowNum, colNum);
        delete [] allCellsArray;
        return cells;
    } else {
        return CreateEmptyCells(0, 0);
    }
}

Cells GetExtendedNodeCells(const Cells& cells, int rowNum, int colNum, int numberOfNodes, int rank) {
    Cells extNodeCells;
    Cells nodeCells = ScatterCellsAcrossNodes(cells, rowNum, colNum, numberOfNodes, rank);
    extNodeCells.push_back(vector<bool>(colNum, false));
    for (size_t i = 0; i < nodeCells.size(); i++) {
        extNodeCells.push_back(nodeCells[i]);
    }
    extNodeCells.push_back(vector<bool>(colNum, false));
    return extNodeCells;
}

Cells GatherCellsFromAllNodesUsingExtCells(const Cells& extCells,
        int rowNum,
        int colNum,
        int numberOfNodes,
        int rank) {
    Cells nodeCells;
    for (int i = 1; i < extCells.size() - 1; i++) {
        nodeCells.push_back(extCells[i]);
    }
    return GatherCellsFromAllNodes(nodeCells, rowNum, colNum, numberOfNodes, rank);
}

int main(int argc, char **argv) {
    int rank, numberOfNodes, len, tag = 1;
    char host[MPI_MAX_PROCESSOR_NAME];

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &numberOfNodes);
    MPI_Get_processor_name(host, &len);

    int numberOfRows;
    int numberOfColumns;
    int numbersBuff[2];

    Cells cells;
    if (rank == 0) {
        cells = ReadCellsFromFile("cells");
        numbersBuff[0] = cells.size();
        numbersBuff[1] = cells[0].size();
        MPI_Bcast(numbersBuff, 2, MPI_INT, 0, MPI_COMM_WORLD);
    } else {
        cells = CreateEmptyCells(0, 0);
        MPI_Bcast(numbersBuff, 2, MPI_INT, 0, MPI_COMM_WORLD);
    }
    numberOfRows = numbersBuff[0];
    numberOfColumns = numbersBuff[1];
    Cells extNodeCells = GetExtendedNodeCells(cells,
            numberOfRows,
            numberOfColumns,
            numberOfNodes,
            rank);
    double start = MPI_Wtime();
    DoManyLifeSteps(&extNodeCells, 4*3, numberOfNodes, rank);
    double end = MPI_Wtime();
    Cells allCells = GatherCellsFromAllNodesUsingExtCells(extNodeCells,
            numberOfRows,
            numberOfColumns,
            numberOfNodes,
            rank);
    if (rank == 0) {
        cout << "time = " << (end - start) << "\n";
        WriteToConsole(allCells);
    }
    MPI_Finalize();
    return 0;
}