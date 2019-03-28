// TPMMS (Two Pass Multiway Merge Sort) Algorithm Implementation
// Written by Sina Pilehchiha [sina.pilehchiha@mail.concordia.ca]
// January - February 2019

#include <iostream> // This header is part of the Input/output library.
#include <string>
//#include <cstdlib>
#include <fstream> // This header is also part of the Input/Output library.
//#include <vector>
//#include <math.h>
#include <algorithm>
//#include <stdlib.h>
#include <string.h>
#include <sstream> // This header is, too, part of the Input/Output library.
#include <queue> // This header is part of the containers library.
//#include <cstdio>
//#include <stdio.h>
#include <errno.h>
//#include <sys/stat.h>
//#include <sys/types.h>
//#include <unistd.h>
#include <libgen.h> // This header concerns the basename() function.
#include <sys/resource.h>
#include <sys/time.h>

// A Claim record with its attributes is defined as a new data type.
// This data struct is used for the first pass of the algorithm.
struct Claim {
    
    char ClaimNumber[9];
    char ClaimDate[11];
    char clientID[10];
    char clientName[26];
    char clientAddress[151];
    char clientEmailAddress[29];
    char insuredItemID[3];
    char damageAmount[10];
    char compensationAmount[11];
    
    //    overload the << operator for writing a Claim record.
    friend std::ostream& operator<<(std::ostream &os, const Claim &Claim)
    {
        os << Claim.ClaimNumber << Claim.ClaimDate << Claim.clientID << Claim.clientName << Claim.clientAddress << Claim.clientEmailAddress << Claim.insuredItemID << Claim.damageAmount << Claim.compensationAmount;
        return os;
    }
    
    //  overload the >> operator for reading into a Claim record.
    friend std::istream& operator>>(std::istream &is, Claim &Claim)
    {
        is.get(Claim.ClaimNumber, 9);
        is.get(Claim.ClaimDate, 11);
        is.get(Claim.clientID, 10);
        is.get(Claim.clientName, 26);
        is.get(Claim.clientAddress, 151);
        is.get(Claim.clientEmailAddress, 29);
        is.get(Claim.insuredItemID, 3);
        is.get(Claim.damageAmount, 10);
        is.get(Claim.compensationAmount, 11);
        is.ignore(1); // ignore the whitespace character at the end of each record of input
        return is;
    }
};

//  The datatype struct used by the priority_queue priorityQueue
struct ClaimForPass2 {
    
    Claim datum; //  data
    std::istream* inputStream;
    bool (*comparisonFunction)(const Claim &c1, const Claim &c2);
    ClaimForPass2 (const Claim &datum, //    constructor
                   std::istream* inputStream,
                   bool (*comparisonFunction)(const Claim &c1, const Claim &c2))
    :
    datum(datum),
    inputStream(inputStream),
    comparisonFunction(comparisonFunction) {}
    
    bool operator < (const ClaimForPass2 &claim) const
    {
        // Priority queues try to sort from highest to lowest. Eergo, one needs to negate the final return value.
        return !(comparisonFunction(datum, claim.datum));
    }
};

bool byClientID(Claim const &c1, Claim const &c2) {
    if (atoi(c1.clientID) < atoi(c2.clientID))  return true;
    else return false;
}

struct TPMMS {
    TPMMS(const std::string &inFile, // constructor, using Claim's overloaded < operator.  Must be defined.
          const std::string &outFile,
          const std::string  &maxBufferSize,
          std::string tempPath,
          bool (*compareFunction)(const Claim &c1, const Claim &c2) = NULL);
    
    ~TPMMS(void); //    destructor
    
    void Sort(); // Sort the data
    
    std::string _inFile;
    bool (*_compareFunction)(const Claim &c1, const Claim &c2);
    std::string _tempPath;
    std::vector<std::string> temporaryFilesNamesList;
    std::vector<std::ifstream*> temporaryFilesList;
    std::string _maxBufferSize;
    unsigned int _chunkCounter;
    std::string _outFile;
    void Pass1(); //    drives the creation of sorted sub-files stored on disk.
    void Pass2(); //    drives the merging of the sorted temp files.
    void WriteToTempFile(const std::vector<Claim> &lines); //   final, sorted and merged output is written to an output file.
    void OpenTempFiles();
    void CloseTemporaryFiles();
    void SumOfCompensationAmounts();
    void ShowTopTenCostliestClients();
};

TPMMS::TPMMS (const std::string &inFile, // constructor
              const std::string &outFile,
              const std::string  &maxBufferSize,
              std::string tempPath,
              bool (*compareFunction)(const Claim &c1, const Claim &c2))
: _inFile(inFile)
, _outFile(outFile)
, _tempPath(tempPath)
, _maxBufferSize(maxBufferSize)
, _compareFunction(compareFunction)
, _chunkCounter(0) {}

TPMMS::~TPMMS(void) {} //   destructor

void TPMMS::Sort() { // API for sorting.
    Pass1();
    Pass2();
}

std::string stl_basename(const std::string &path) { //   STLized version of basename() (because POSIX basename() modifies the input string pointer.)
    std::string result;
    char* path_dup = strdup(path.c_str());
    char* basename_part = basename(path_dup);
    result = basename_part;
    free(path_dup);
    size_t pos = result.find_last_of('.');
    if (pos != std::string::npos ) // checks whether pos is at the end of the sting or not.
        result = result.substr(0,pos); // updates the length of result.
    return result;
}

void TPMMS::OpenTempFiles() {
    for (size_t i=0; i < temporaryFilesNamesList.size(); ++i) {
        std::ifstream* file;
        file = new std::ifstream(temporaryFilesNamesList[i].c_str(), std::ios::in);
        if (file->good() == true) {
            temporaryFilesList.push_back(file); // add a pointer to the opened temp file to the list
        }
        else {
            std::cerr << "Unable to open temp file (" << temporaryFilesNamesList[i]
            << ").  I suspect a limit on number of open file handles.  Exiting."
            << std::endl;
            CloseTemporaryFiles();
            exit(1);
        }
    }
}

void TPMMS::CloseTemporaryFiles() {
    for (size_t i=0; i < temporaryFilesList.size(); ++i) { //  delete the pointers to the temp files.
        temporaryFilesList[i]->close();
        delete temporaryFilesList[i];
    }
    for (size_t i=0; i < temporaryFilesNamesList.size(); ++i) { //  delete the temp files from the file system.
        remove(temporaryFilesNamesList[i].c_str());  // remove = UNIX "rm"
    }
}

void TPMMS::WriteToTempFile(const std::vector<Claim> &buffer) {
    std::stringstream tempFileSS; //    name the current tempfile
    if (_tempPath.size() == 0)
        tempFileSS << _inFile << "." << _chunkCounter;
    else
        tempFileSS << _tempPath << "/" << stl_basename(_inFile) << "." << _chunkCounter;
    std::string temporaryFileName = tempFileSS.str();
    std::ofstream* output;
    output = new std::ofstream(temporaryFileName, std::ios::out);
    for (size_t i = 0; i < buffer.size(); ++i) { // Write the contents of the current buffer to the temporary file.
        *output << buffer[i] << std::endl;
    }
    ++_chunkCounter; //   update the tempFile number and add the tempFile to the list of tempFiles
    output->close();
    delete output;
    temporaryFilesNamesList.push_back(temporaryFileName);
}

void TPMMS::Pass1() {
    std::istream* input = new std::ifstream(_inFile.c_str(), std::ios::in);
    std::vector<Claim> buffer;
    if (_maxBufferSize == "0") {std::cerr << "Seriously? You want me to do merge sort with a buffer of size 0?" << std::endl; exit(1);}
    buffer.reserve(stoi(_maxBufferSize));
    unsigned int totalBytes = 0;  // track the number of bytes consumed so far.
    Claim record;
    while (*input >> record) { // keep reading until there is no more input data
        buffer.push_back(record); //  add the current record to the buffer and
        //totalBytes += sizeof(line);
        totalBytes += sizeof(record); //  track the memory used.
        if (totalBytes > (stoi(_maxBufferSize) * sizeof(record)) - sizeof(record)) { //    sort the buffer and write to a temp file if we have filled up our quota
            sort(buffer.begin(), buffer.end(), byClientID); //  sort the buffer.
            WriteToTempFile(buffer); // write the sorted data to a temp file
            buffer.clear(); //  clear the buffer for the next run
            totalBytes = 0; // make the totalBytes counter zero in order to count the bytes occupying the buffer again.
        }
    }
    
    if (buffer.empty() == false) {  //  handle the run (if any) from the last chunk of the input file.
        sort(buffer.begin(), buffer.end(), byClientID);
        WriteToTempFile(buffer); // write the sorted data to a temp file
        buffer.clear();
    }
    buffer.shrink_to_fit();
    std::cout << "Phase 1 completed..." << std::endl;
}

void TPMMS::Pass2() { //    Merge the sorted temp files.
    // uses a priority queue, with the values being a pair of the record from the file, and the stream from which the record came
    // open the sorted temp files up for merging.
    // loads ifstream pointers into temporaryFilesList
    std::ostream *output = new std::ofstream(_outFile.c_str(), std::ios::out);
    OpenTempFiles();
    
    //  A priority queue is a container adaptor
    //  that provides constant time lookup of the largest (by default) element,
    //  at the expense of logarithmic insertion and extraction.
    std::priority_queue<ClaimForPass2> priorityQueue; //  priority queue for the buffer.
    Claim record; //  extract the first record from each temp file
    for (size_t i = 0; i < temporaryFilesList.size(); ++i) {
        *temporaryFilesList[i] >> record;
        priorityQueue.push(ClaimForPass2(record, temporaryFilesList[i], byClientID));
    }
    while (priorityQueue.empty() == false) { //  keep working until the queue is empty
        ClaimForPass2 lowest = priorityQueue.top();  //   grab the lowest element, print it, then ditch it.
        *output << lowest.datum << std::endl; //    write the entry from the top of the queue
        priorityQueue.pop(); //  remove this record from the queue
        *(lowest.inputStream) >> record; //    add the next record from the lowest stream (above) to the queue as long as it's not EOF.
        if (*(lowest.inputStream))
            priorityQueue.push( ClaimForPass2(record, lowest.inputStream, byClientID) );
    }
    CloseTemporaryFiles();  // Clean up the temporary files.
    std::cout << "Phase 2 completed...\nGoing to sum compensation amounts..." << std::endl;
}

void TPMMS::SumOfCompensationAmounts() {
    std::istream* input  = new std::ifstream(_outFile.c_str(), std::ios::in);
    std::ofstream SumOfCompensationAmountsFile;
    SumOfCompensationAmountsFile.open("SumOfCompensationAmountsFile.txt");
    Claim initialRecord, record;
    *input >> initialRecord;
    while (*input >> record) { // keep reading until there is no more input data
        if (std::string(initialRecord.clientID) == std::string(record.clientID)) {
            sprintf(initialRecord.compensationAmount, "%.2f", atof(initialRecord.compensationAmount) + atof(record.compensationAmount));
        }
        else {
            SumOfCompensationAmountsFile << initialRecord << std::endl;
            initialRecord = record;
        }
    }
    SumOfCompensationAmountsFile << initialRecord << std::endl;
    SumOfCompensationAmountsFile.close();
    std::cout << "Summed compensation amounts...\n" << std::endl;
}

// A Claim record with its attributes is defined as a new data type.
// This data struct is used for the first pass of the algorithm.
struct Claim2 {
    
    char ClaimNumber[9];
    char ClaimDate[11];
    char clientID[10];
    char clientName[26];
    char clientAddress[151];
    char clientEmailAddress[29];
    char insuredItemID[3];
    char damageAmount[10];
    char compensationAmount[19];
    
    //   overload the < (less than) operator for comparison between Claim records.
    
    //    overload the << operator for writing a Claim record.
    friend std::ostream& operator<<(std::ostream &os, const Claim2 &Claim)
    {
        //  The reason for inserting whitespace into the output stream is to retain the initial data format from the input file.
        os << Claim.ClaimNumber << Claim.ClaimDate << Claim.clientID << Claim.clientName << Claim.clientAddress << Claim.clientEmailAddress << Claim.insuredItemID << Claim.damageAmount << Claim.compensationAmount;
        return os;
    }
    
    //  overload the >> operator for reading into a Claim record.
    friend std::istream& operator>>(std::istream &is, Claim2 &Claim)
    {
        is.get(Claim.ClaimNumber, 9);
        is.get(Claim.ClaimDate, 11);
        is.get(Claim.clientID, 10);
        is.get(Claim.clientName, 26);
        is.get(Claim.clientAddress, 151);
        is.get(Claim.clientEmailAddress, 29);
        is.get(Claim.insuredItemID, 3);
        is.get(Claim.damageAmount, 10);
        is.get(Claim.compensationAmount, 19);
        is.ignore(1); // ignore the whitespace character at the end of each record of input
        return is;
    }
};

//  the datatype struct used by the priority_queue
struct ClaimForPass22 {
    
    Claim2 datum; //  data
    std::istream* stream;
    bool (*comparisonFunction)(const Claim2 &c1, const Claim2 &c2);
    ClaimForPass22 (const Claim2 &datum, //    constructor
                    std::istream* stream,
                    bool (*comparisonFunction)(const Claim2 &c1, const Claim2 &c2))
    :
    datum(datum),
    stream(stream),
    comparisonFunction(comparisonFunction) {}
    
    bool operator < (const ClaimForPass22 &c) const
    {
        //  recall that priority queues try to sort from highest to lowest. thus, we need to negate.
        return !(comparisonFunction(datum, c.datum));
    }
};

// Comparison function for sorting by compensationAmount
bool byCompensationAmount(Claim2 const &c1, Claim2 const &c2) {
    return (atof(c1.compensationAmount) > atof(c2.compensationAmount));
}

struct TPMMS2 {
    TPMMS2(const std::string &inFile, // constructor, using Claim's overloaded < operator.  Must be defined.
           const std::string &outFile,
           const std::string  &maxBufferSize,
           std::string tempPath,
           bool (*compareFunction)(const Claim2 &c1, const Claim2 &c2) = NULL);
    
    ~TPMMS2(void); //    destructor
    
    void Sort2(); // Sort the data
    
    std::string _inFile;
    bool (*_compareFunction)(const Claim2 &c1, const Claim2 &c2);
    std::string _tempPath;
    std::vector<std::string> temporaryFilesNamesList;
    std::vector<std::ifstream*> temporaryFilesList;
    std::string _maxBufferSize;
    unsigned int _chunkCounter;
    std::string _outFile;
    void Pass12(); //    drives the creation of sorted sub-files stored on disk.
    void Pass22(); //    drives the merging of the sorted temp files.
    void WriteToTempFile(const std::vector<Claim2> &lines); //   final, sorted and merged output is written to an output file.
    void OpenTempFiles();
    void CloseTemporaryFiles();
    void SumOfCompensationAmounts();
    void ShowTopTenCostliestClients();
};
TPMMS2::TPMMS2 (const std::string &inFile, // constructor
                const std::string &outFile,
                const std::string  &maxBufferSize,
                std::string tempPath,
                bool (*compareFunction)(const Claim2 &c1, const Claim2 &c2))
: _inFile(inFile)
, _outFile(outFile)
, _tempPath(tempPath)
, _maxBufferSize(maxBufferSize)
, _compareFunction(compareFunction)
, _chunkCounter(0) {}

TPMMS2::~TPMMS2(void) {} //   destructor

void TPMMS2::Sort2() { // API for sorting.
    Pass12();
    Pass22();
}

void TPMMS2::OpenTempFiles() {
    for (size_t i=0; i < temporaryFilesNamesList.size(); ++i) {
        std::ifstream* file;
        file = new std::ifstream(temporaryFilesNamesList[i].c_str(), std::ios::in);
        if (file->good() == true) {
            temporaryFilesList.push_back(file); // add a pointer to the opened temp file to the list
        }
        else {
            std::cerr << "Unable to open temp file (" << temporaryFilesNamesList[i]
            << ").  I suspect a limit on number of open file handles.  Exiting."
            << std::endl;
            CloseTemporaryFiles();
            exit(1);
        }
    }
}

void TPMMS2::CloseTemporaryFiles() {
    for (size_t i=0; i < temporaryFilesList.size(); ++i) { //  delete the pointers to the temp files.
        temporaryFilesList[i]->close();
        delete temporaryFilesList[i];
    }
    for (size_t i=0; i < temporaryFilesNamesList.size(); ++i) { //  delete the temp files from the file system.
        remove(temporaryFilesNamesList[i].c_str());  // remove = UNIX "rm"
    }
}

void TPMMS2::WriteToTempFile(const std::vector<Claim2> &buffer) {
    std::stringstream tempFileSS; //    name the current tempfile
    if (_tempPath.size() == 0)
        tempFileSS << _inFile << "." << _chunkCounter;
    else
        tempFileSS << _tempPath << "/" << stl_basename(_inFile) << "." << _chunkCounter;
    std::string temporaryFileName = tempFileSS.str();
    std::ofstream* output;
    output = new std::ofstream(temporaryFileName, std::ios::out);
    for (size_t i = 0; i < buffer.size(); ++i) { // write the contents of the current buffer to the temp file
        *output << buffer[i] << std::endl;
    }
    ++_chunkCounter; //   update the tempFile number and add the tempFile to the list of tempFiles
    output->close();
    delete output;
    temporaryFilesNamesList.push_back(temporaryFileName);
}

void TPMMS2::Pass12() {
    std::istream* input = new std::ifstream(_inFile.c_str(), std::ios::in);
    std::vector<Claim2> buffer;
    if (_maxBufferSize == "0") {std::cerr << "Seriously? You want me to do merge sort with a buffer of size 0?" << std::endl; exit(1);}
    buffer.reserve(stoi(_maxBufferSize));
    unsigned int totalBytes = 0;  // track the number of bytes consumed so far.
    Claim2 record;
    while (*input >> record) { // keep reading until there is no more input data
        buffer.push_back(record); //  add the current record to the buffer and
        //totalBytes += sizeof(line);
        totalBytes += sizeof(record); //  track the memory used.
        if (totalBytes > (stoi(_maxBufferSize) * sizeof(record)) - sizeof(record)) { //    sort the buffer and write to a temp file if we have filled up our quota
            sort(buffer.begin(), buffer.end(), byCompensationAmount); //  sort the buffer.
            WriteToTempFile(buffer); // write the sorted data to a temp file
            buffer.clear(); //  clear the buffer for the next run
            totalBytes = 0; // make the totalBytes counter zero in order to count the bytes occupying the buffer again.
        }
    }
    if (buffer.empty() == false) {  //  handle the run (if any) from the last chunk of the input file.
        sort(buffer.begin(), buffer.end(), byCompensationAmount);
        WriteToTempFile(buffer); // write the sorted data to a temp file
        buffer.clear();
    }
    buffer.shrink_to_fit();
}

void TPMMS2::Pass22() { // Merge the sorted temporary sublists.
    // Use a priority queue, with the values being a pair of the record from the file, and the stream from which the record came.
    // Open the sorted temporary sublists for merging.
    // Load ifstream pointers into temporaryFilesList
    std::ostream *output = new std::ofstream(_outFile.c_str(), std::ios::out);
    OpenTempFiles();
    
    //  A priority queue is a container adaptor
    //  That provides constant time lookup of the largest (by default) element,
    //  At the expense of logarithmic insertion and extraction.
    std::priority_queue<ClaimForPass22> priorityQueue; //  priority queue for the buffer.
    Claim2 record; //  extract the first record from each temp file
    for (size_t i = 0; i < temporaryFilesList.size(); ++i) {
        *temporaryFilesList[i] >> record;
        priorityQueue.push(ClaimForPass22(record, temporaryFilesList[i], byCompensationAmount));
    }
    while (priorityQueue.empty() == false) { // Keep working until the queue is empty
        ClaimForPass22 lowest = priorityQueue.top();  // Grab the lowest element, print it, then ditch it.
        *output << lowest.datum << std::endl; // Write the entry from the top of the queue
        priorityQueue.pop(); // Remove this record from the queue
        *(lowest.stream) >> record; // Add the next record from the lowest stream (above) to the queue as long as it's not EOF.
        if (*(lowest.stream))
            priorityQueue.push(ClaimForPass22(record, lowest.stream, byCompensationAmount));
    }
    CloseTemporaryFiles();  // Clean up the temporary files.
}

void TPMMS2::ShowTopTenCostliestClients() {
    const std::string outFile2;
    std::istream* input = new std::ifstream(_outFile.c_str(), std::ios::in);
    Claim2 record;
    std::cout << "Client ID" << "\t" << "Sum of Compensation Amount\n\n";
    for(unsigned short i = 0; i < 10; i++) {
        *input >> record;
        std::cout << record.clientID << "\t" << record.compensationAmount << std::endl;
    }
}

// A program shall contain a global function named main, which is the designated start of the program.
int main(int argc, char* argv[]) {
    int who = RUSAGE_SELF;
    struct rusage usage;
    int ret;
    ret = getrusage(who, &usage);
    //limit.rlim_max = RLIM_INFINITY; // send SIGKILL after 3 seconds
    //setrlimit(RUSAGE_SELF, &limit);
    // This argument is given to the executable pogram via the command line interface.
    std::string inputFile = argv[1];
    
    // Allow the sorter to use an arbitrary amount (in MegaBytes) of memory for sorting.
    std::string bufferSize = argv[2];
    
    // Once the buffer is full, the sorter will dump the buffer's content to a temporary file and grab another chunk from the input file.
    std::string temporaryPath = argv[3]; // Allows you to write the intermediate files anywhere you want.
    
    const clock_t BEGINNING = clock(); // Mark the beginning of the execution of the sorting procedure.
    // Create a new instance of the TPMMS class.
    TPMMS* firstSorter = new TPMMS (inputFile, "outputFile.txt", bufferSize, temporaryPath, byClientID) ;
    firstSorter->Sort();
    firstSorter->SumOfCompensationAmounts();
    
    TPMMS2* secondSorter = new TPMMS2 ("SumOfCompensationAmountsFile.txt", "outputFile2.txt", bufferSize, temporaryPath, byCompensationAmount);
    secondSorter->Sort2();
    
    const double EXECUTION_TIME = (double)(clock() - BEGINNING) / CLOCKS_PER_SEC / 60; // Report the execution time (in minutes).
    
    secondSorter->ShowTopTenCostliestClients();
    
    std::cout << "\n" << "Execution time in minutes:\t" << EXECUTION_TIME << "\n"; // Print out the time elapsed sorting.
}
